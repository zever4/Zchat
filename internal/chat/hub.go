package chat

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/metrics"
)

// DirectMessage routes a message to specific users
type DirectMessage struct {
	RecipientIDs []int64
	Payload      []byte
	// Receipts belong to the originating connection, not other devices/tabs
	// which may still run legacy clients. Only the hub touches its Send channel.
	Connection *Client
}

// Hub maintains set of active clients and broadcasts messages
type Hub struct {
	// Registered clients.
	Clients map[*Client]bool

	// All connections for each user, owned by the hub loop.
	UserClients map[int64]map[*Client]struct{}

	// Targeted messages to specific users
	Direct chan *DirectMessage

	// Register requests from the clients.
	Register chan *Client

	// Unregister requests from clients.
	Unregister chan *Client

	revoke   chan revokeRequest
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	finished chan struct{}
	pumps    sync.WaitGroup
}

func NewHub() *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		Clients:     make(map[*Client]bool),
		UserClients: make(map[int64]map[*Client]struct{}),
		Direct:      make(chan *DirectMessage, 256),
		Register:    make(chan *Client),
		Unregister:  make(chan *Client),
		revoke:      make(chan revokeRequest),
		ctx:         ctx, cancel: cancel, done: make(chan struct{}), finished: make(chan struct{}),
	}
}

func (h *Hub) Run(ctx context.Context) {
	h.run(ctx)
}

func (h *Hub) run(ctx context.Context) {
	defer func() {
		h.cancel()
		for client := range h.Clients {
			h.removeClient(client)
		}
		close(h.done) // All sockets are closed before a revoke caller is released.
		h.pumps.Wait()
		close(h.finished)
	}()
	for {
		if ctx.Err() != nil || h.ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-h.ctx.Done():
			return
		case req := <-h.revoke:
			for client := range h.UserClients[req.userID] {
				if req.sessionID == "" || client.sessionID == req.sessionID {
					h.removeClient(client)
				}
			}
			close(req.done)
		case client := <-h.Register:
			if h.Clients[client] {
				continue
			}
			h.Clients[client] = true
			if h.UserClients[client.ID] == nil {
				h.UserClients[client.ID] = make(map[*Client]struct{})
			}
			h.UserClients[client.ID][client] = struct{}{}
			metrics.WebsocketConnections.Inc()
			h.GreetClient(client)
			if client.Conn != nil {
				h.pumps.Add(2)
				go func() { defer h.pumps.Done(); client.WritePump() }()
				go func() { defer h.pumps.Done(); client.ReadPump() }()
			}
		case client := <-h.Unregister:
			h.removeClient(client)
		case dm := <-h.Direct:
			if dm.Connection != nil {
				if h.Clients[dm.Connection] {
					h.sendToClient(dm.Connection, dm.Payload)
				}
				continue
			}
			for _, uid := range dm.RecipientIDs {
				for client := range h.UserClients[uid] {
					h.sendToClient(client, dm.Payload)
				}
			}
		}
	}
}

// Only the hub loop closes Send or changes connection membership.
func (h *Hub) removeClient(client *Client) {
	if !h.Clients[client] {
		return
	}
	delete(h.Clients, client)
	delete(h.UserClients[client.ID], client)
	if len(h.UserClients[client.ID]) == 0 {
		delete(h.UserClients, client.ID)
	}
	if client.Conn != nil {
		if client.cancel != nil {
			client.cancel()
		}
		_ = client.Conn.Close()
	}
	close(client.Send)
	metrics.WebsocketConnections.Dec()
}

func (h *Hub) sendToClient(client *Client, payload []byte) {
	if !client.expiresAt.IsZero() && !time.Now().Before(client.expiresAt) {
		h.removeClient(client)
		return
	}
	if cap(client.Send) > 0 {
		metrics.WebsocketQueueFill.Observe(float64(len(client.Send)) / float64(cap(client.Send)))
	}
	select {
	case client.Send <- payload:
	default:
		metrics.WebsocketSlowClients.Inc()
		h.removeClient(client)
	}
}

func (h *Hub) GreetClient(c *Client) {
	msg := map[string]any{"type": domain.MessageTypeSystem, "sender_name": "Zchat",
		"content": "Добро пожаловать!", "created_at": time.Now(), "message_receipts": true}

	message, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.sendToClient(c, message)
}

type revokeRequest struct {
	userID    int64
	sessionID string
	done      chan struct{}
}

// DisconnectUser returns only after the owner loop has closed every current socket.
// Caller holds AuthService's revocation lock to exclude delayed registrations.
func (h *Hub) DisconnectUser(userID int64) {
	h.DisconnectSession(userID, "")
}

func (h *Hub) DisconnectSession(userID int64, sessionID string) {
	done := make(chan struct{})
	select {
	case h.revoke <- revokeRequest{userID: userID, sessionID: sessionID, done: done}:
	case <-h.done:
		return
	}
	select {
	case <-done:
	case <-h.done:
	}
}

// Stop rejects new work; Wait also waits for all WebSocket pumps to return.
func (h *Hub) Stop() { h.cancel() }
func (h *Hub) Wait(ctx context.Context) error {
	select {
	case <-h.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *Hub) RegisterClient(ctx context.Context, c *Client) bool {
	if h.ctx.Err() != nil || ctx.Err() != nil {
		return false
	}
	select {
	case h.Register <- c:
		return true
	case <-h.ctx.Done():
		return false
	case <-ctx.Done():
		return false
	}
}
func (h *Hub) UnregisterClient(c *Client) {
	select {
	case h.Unregister <- c:
	case <-h.ctx.Done():
	}
}
func (h *Hub) Enqueue(ctx context.Context, dm *DirectMessage) (queued bool) {
	defer func() {
		if !queued {
			metrics.WebsocketEnqueueFailures.Inc()
		}
	}()
	if ctx.Err() != nil || (h.ctx != nil && h.ctx.Err() != nil) {
		return false
	}
	var stopped <-chan struct{}
	if h.ctx != nil {
		stopped = h.ctx.Done()
	}
	select {
	case h.Direct <- dm:
		return true
	case <-stopped:
		return false
	case <-ctx.Done():
		return false
	}
}

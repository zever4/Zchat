package chat

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/auth"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 2048
)

var (
	newline = []byte{'\n'}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:   1024,
	WriteBufferSize:  1024,
	HandshakeTimeout: 5 * time.Second,
	CheckOrigin:      auth.SameOrigin,
}

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	Hub *Hub

	// Websocket connection
	Conn *websocket.Conn

	Send chan []byte

	// User info
	Username  string
	ID        int64
	AvatarURL string

	ChatRepo    repository.ChatRepository
	MessageRepo repository.MessageRepository
	authService *service.AuthService
	token       string
	sessionID   string
	expiresAt   time.Time
	ctx         context.Context
	cancel      context.CancelFunc
}

// incomingMessage is the JSON format sent by the frontend
type incomingMessage struct {
	ChatID          int64  `json:"chat_id"`
	Content         string `json:"content"`
	Type            string `json:"type,omitempty"`
	ClientMessageID string `json:"client_message_id,omitempty"`
}

var clientMessageIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func (c *Client) ReadPump() {
	defer func() {
		c.Conn.Close()
		if c.cancel != nil {
			c.cancel()
		}
		c.Hub.UnregisterClient(c)
	}()
	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(string) error { c.Conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	for {
		_, rawMessage, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				// Close reasons are supplied by the peer and may contain private data.
				log.Printf("WebSocket read failed (user_id=%d, error_type=%T)", c.ID, err)
			}
			break
		}

		// Parse incoming JSON message
		var incoming incomingMessage
		if err := json.Unmarshal(rawMessage, &incoming); err != nil {
			metrics.WebsocketFrames.WithLabelValues("invalid_json").Inc()
			log.Printf("Invalid WebSocket JSON (user_id=%d, bytes=%d)", c.ID, len(rawMessage))
			continue
		}

		kind := "message"
		switch incoming.Type {
		case "typing":
			kind = "typing"
		case "call_invite", "call_accept", "call_reject", "call_end":
			kind = "call_signal"
		}
		metrics.WebsocketFrames.WithLabelValues(kind).Inc()
		if !c.processAuthenticated(incoming) {
			return
		}
	}
}

// Session validation and processing share the revocation barrier. No queued frame
// can write after a successful logout-all response, even if read before revocation.
func (c *Client) processAuthenticated(incoming incomingMessage) bool {
	ctx, cancel := context.WithTimeout(c.baseContext(), 3*time.Second)
	defer cancel()
	admit := c.authService.AdmitMessage
	switch incoming.Type {
	case "call_invite", "call_accept", "call_reject", "call_end":
		admit = c.authService.AdmitCallSignal
	}
	release, err := admit(ctx, c.ID)
	if err != nil {
		if errors.Is(err, admission.ErrBusy) {
			// Rejection precedes DB authorization/persistence. Keep the socket
			// and the sender's exact outbox UUID/body for a jittered retry.
			if incoming.Type != "typing" {
				c.messageError(incoming, "server_busy", "Сервер занят. Повторите отправку", true)
			}
			return true
		}
		return false
	}
	defer release()
	unlock, err := c.authService.LockSessionContext(ctx, c.ID)
	if err != nil {
		return false
	}
	defer unlock()
	id, err := c.authService.ValidateTokenContext(ctx, c.token)
	if err != nil || id != c.ID {
		return false
	}
	switch incoming.Type {
	case "typing":
		c.broadcastTyping(ctx, incoming.ChatID)
	case "call_invite", "call_accept", "call_reject", "call_end":
		c.broadcastCallSignal(ctx, incoming.ChatID, incoming.Type, incoming.Content)
	default:
		c.handleMessageContext(ctx, incoming)
	}
	return true
}

// handleMessage authorizes a persistent message using server-owned chat metadata.
func (c *Client) handleMessage(incoming incomingMessage) {
	c.handleMessageContext(c.baseContext(), incoming)
}
func (c *Client) baseContext() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}
func (c *Client) handleMessageContext(parent context.Context, incoming incomingMessage) {
	start := time.Now()
	result := "invalid"
	defer func() {
		metrics.WebsocketMessageDuration.Observe(time.Since(start).Seconds())
		metrics.WebsocketMessageResults.WithLabelValues(result).Inc()
	}()
	content := html.EscapeString(strings.TrimSpace(incoming.Content))
	if incoming.ChatID <= 0 || len(content) == 0 || len(content) >= 1500 ||
		(incoming.ClientMessageID != "" && !clientMessageIDPattern.MatchString(incoming.ClientMessageID)) {
		c.messageError(incoming, "invalid_message", "Сообщение слишком длинное или имеет неверный формат", false)
		return
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	msg := domain.Message{
		SenderID:        c.ID,
		SenderName:      c.Username,
		SenderAvatar:    c.AvatarURL,
		ChatID:          incoming.ChatID,
		Content:         content,
		ClientMessageID: incoming.ClientMessageID,
		CreatedAt:       time.Now(),
	}

	chatInfo, memberIDs, err := c.MessageRepo.SaveAuthorized(ctx, &msg)
	result = "saved"
	if err != nil {
		result = "error"
		switch {
		case errors.Is(err, domain.ErrChatWriteForbidden), errors.Is(err, domain.ErrNotChatMember), errors.Is(err, domain.ErrChatNotFound):
			result = "forbidden"
		case errors.Is(err, context.DeadlineExceeded):
			result = "timeout"
		case errors.Is(err, context.Canceled):
			result = "canceled"
		case errors.Is(err, domain.ErrMessageIDConflict), errors.Is(err, domain.ErrInvalidInput):
			result = "invalid"
		}
		switch {
		case errors.Is(err, domain.ErrChatWriteForbidden):
			c.messageError(incoming, "forbidden", "Только администраторы могут писать в этот чат", false)
		case errors.Is(err, domain.ErrNotChatMember), errors.Is(err, domain.ErrChatNotFound):
			c.messageError(incoming, "forbidden", "Нет доступа к отправке в этот чат", false)
		case errors.Is(err, domain.ErrMessageIDConflict):
			c.messageError(incoming, "id_conflict", "Идентификатор уже использован для другого сообщения", false)
		case errors.Is(err, domain.ErrInvalidInput):
			c.messageError(incoming, "invalid_message", "Сообщение или вложение недоступно", false)
		default:
			// A timeout during COMMIT is ambiguous. A retry must keep the same ID.
			c.messageError(incoming, "save_unconfirmed", "Сохранение не подтверждено. Повтор безопасен", true)
			log.Printf("Failed to authorize/save message: %v", err)
		}
		return
	}
	if msg.Replayed {
		result = "replayed"
		c.messageAck(incoming, msg)
		return
	}

	broadcastMsg := msg
	if chatInfo.Type != "" {
		broadcastMsg.Type = chatInfo.Type
	} else {
		broadcastMsg.Type = domain.MessageTypePrivate
	}
	if chatInfo.Type == "group" && chatInfo.AnonymousMode {
		broadcastMsg.SenderID = 0
		broadcastMsg.SenderName = chatInfo.Name
		broadcastMsg.SenderAvatar = chatInfo.AvatarURL
	}
	payload, err := json.Marshal(broadcastMsg)
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return
	}
	metrics.WebsocketMessagesTotal.WithLabelValues("from_client").Inc()
	if !c.Hub.Enqueue(ctx, &DirectMessage{RecipientIDs: memberIDs, Payload: payload}) {
		log.Printf("Message %d saved but broadcast timed out", msg.ID)
	}
	c.messageAck(incoming, msg)
}

func (c *Client) messageAck(incoming incomingMessage, msg domain.Message) {
	if incoming.ClientMessageID == "" {
		return
	}
	c.messageReceipt(map[string]any{"type": "message_ack", "chat_id": msg.ChatID,
		"client_message_id": incoming.ClientMessageID, "id": msg.ID, "created_at": msg.CreatedAt})
}

func (c *Client) messageError(incoming incomingMessage, code, message string, retryable bool) {
	// Legacy clients have no correlated outbox, but still receive the reason.
	c.messageReceipt(map[string]any{"type": "error", "chat_id": incoming.ChatID,
		"client_message_id": incoming.ClientMessageID, "code": code, "message": message, "retryable": retryable})
}

func (c *Client) messageReceipt(value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		return
	}
	// The SQL/request deadline may have expired. Use the live connection's
	// context with a separate short budget; only the hub writes to Send.
	ctx, cancel := context.WithTimeout(c.baseContext(), 250*time.Millisecond)
	defer cancel()
	c.Hub.Enqueue(ctx, &DirectMessage{RecipientIDs: []int64{c.ID}, Connection: c, Payload: payload})
}

func (c *Client) broadcastTyping(ctx context.Context, chatID int64) {
	senderID := c.ID
	senderName := c.Username
	chatMetaCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	chatInfo, err := c.ChatRepo.GetChatByID(chatMetaCtx, chatID)
	cancel()
	if err != nil || chatInfo == nil {
		return
	}
	if chatInfo.Type == "group" && chatInfo.AnonymousMode {
		senderID = 0
		senderName = "Кто-то"
	}
	payload, err := json.Marshal(map[string]any{
		"type":        "typing",
		"sender_id":   senderID,
		"sender_name": senderName,
		"chat_id":     chatID,
	})
	if err != nil {
		log.Printf("Failed to marshal typing payload: %v", err)
		return
	}
	memberIDs, ok := c.getChatRecipients(ctx, chatID)
	if !ok {
		return
	}
	filtered := make([]int64, 0, len(memberIDs))
	for _, id := range memberIDs {
		if id != c.ID {
			filtered = append(filtered, id)
		}
	}
	c.Hub.Enqueue(ctx, &DirectMessage{
		RecipientIDs: filtered,
		Payload:      payload,
	})
}

func (c *Client) broadcastCallSignal(ctx context.Context, chatID int64, signalType string, content string) {
	memberIDs, ok := c.getChatRecipients(ctx, chatID)
	if !ok {
		return
	}

	// For call_invite: exclude sender (they already know they're calling)
	// For accept/reject/end: send to all including sender (multi-device sync)
	recipientIDs := memberIDs
	if signalType == "call_invite" {
		filtered := make([]int64, 0, len(memberIDs))
		for _, id := range memberIDs {
			if id != c.ID {
				filtered = append(filtered, id)
			}
		}
		recipientIDs = filtered
	}

	payload, err := json.Marshal(map[string]any{
		"type":          signalType,
		"chat_id":       chatID,
		"sender_id":     c.ID,
		"sender_name":   c.Username,
		"sender_avatar": c.AvatarURL,
		"content":       content,
	})
	if err != nil {
		log.Printf("Failed to marshal call signal: %v", err)
		return
	}

	c.Hub.Enqueue(ctx, &DirectMessage{
		RecipientIDs: recipientIDs,
		Payload:      payload,
	})
}

func (c *Client) getChatRecipients(ctx context.Context, chatID int64) ([]int64, bool) {
	isMember, err := c.ChatRepo.IsMember(ctx, chatID, c.ID)
	if err != nil || !isMember {
		return nil, false
	}

	members, err := c.ChatRepo.GetMemberIDs(ctx, chatID)
	if err != nil || len(members) == 0 {
		return nil, false
	}
	return members, true
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	var expiry <-chan time.Time
	if !c.expiresAt.IsZero() {
		timer := time.NewTimer(time.Until(c.expiresAt))
		defer timer.Stop()
		expiry = timer.C
	}
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()
	for {
		select {
		case <-expiry:
			return
		case message, ok := <-c.Send:
			if !c.expiresAt.IsZero() && !time.Now().Before(c.expiresAt) {
				return
			}
			c.setWriteDeadline()
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				metrics.WebsocketWriteErrors.Inc()
				log.Printf("NewWriter error: %v", err)
				return
			}
			if _, err := w.Write(message); err != nil {
				metrics.WebsocketWriteErrors.Inc()
				_ = w.Close()
				return
			}
			// Count a batch only after the entire frame has been written successfully.
			n := len(c.Send)
			for range n {
				if _, err := w.Write(newline); err != nil {
					metrics.WebsocketWriteErrors.Inc()
					_ = w.Close()
					return
				}
				if _, err := w.Write(<-c.Send); err != nil {
					metrics.WebsocketWriteErrors.Inc()
					_ = w.Close()
					return
				}
			}
			if err := w.Close(); err != nil {
				metrics.WebsocketWriteErrors.Inc()
				log.Printf("Close error: %v", err)
				return
			}
			metrics.WebsocketMessagesTotal.WithLabelValues("to_client").Add(float64(n + 1))
		case <-ticker.C:
			if !c.expiresAt.IsZero() && !time.Now().Before(c.expiresAt) {
				return
			}
			c.setWriteDeadline()
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("WriteMessage error: %v", err)
				return
			}
		}
	}
}

func (c *Client) setWriteDeadline() {
	deadline := time.Now().Add(writeWait)
	if !c.expiresAt.IsZero() && c.expiresAt.Before(deadline) {
		deadline = c.expiresAt
	}
	c.Conn.SetWriteDeadline(deadline)
}

func wsAuthenticationError(w http.ResponseWriter, err error) {
	if errors.Is(err, admission.ErrBusy) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Server busy; retry later", http.StatusTooManyRequests)
		return
	}
	if errors.Is(err, service.ErrAuthUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		http.Error(w, "Authentication temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
}

func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request, authService *service.AuthService, userRepo repository.UserRepository, messageRepo repository.MessageRepository, chatRepo repository.ChatRepository) {
	start := time.Now()
	result := "not_registered"
	defer func() { metrics.WebsocketHandshakes.WithLabelValues(result).Observe(time.Since(start).Seconds()) }()
	w.Header().Set("Cache-Control", "private, no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.SameOrigin(r) {
		http.Error(w, "Forbidden Origin", http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	stop := context.AfterFunc(hub.ctx, cancel)
	defer stop()
	if hub.ctx.Err() != nil {
		http.Error(w, "Server stopping", http.StatusServiceUnavailable)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) != 1 || len(query["ticket"]) != 1 || len(r.Header.Values("Authorization")) != 0 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	tokenStr, err := authService.ConsumeWebSocketTicket(query.Get("ticket"), r.Host)
	if err != nil {
		wsAuthenticationError(w, err)
		return
	}

	claims, unlock, err := authService.AuthorizeContext(ctx, tokenStr)
	if err != nil {
		wsAuthenticationError(w, err)
		return
	}
	defer unlock()
	userID := claims.UserID

	user, err := userRepo.GetByID(ctx, userID)
	if err != nil {
		http.Error(w, "User not found", http.StatusForbidden)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed (error_type=%T)", err)
		return
	}

	client := &Client{
		Hub:         hub,
		Conn:        conn,
		Send:        make(chan []byte, 256),
		ID:          user.ID,
		Username:    user.Username,
		AvatarURL:   user.AvatarURL,
		ChatRepo:    chatRepo,
		MessageRepo: messageRepo,
		authService: authService,
		token:       tokenStr,
		sessionID:   claims.ID,
		expiresAt:   claims.ExpiresAt.Time,
	}
	client.ctx, client.cancel = context.WithCancel(hub.ctx)
	if !hub.RegisterClient(ctx, client) {
		client.cancel()
		_ = conn.Close()
		return
	}
	result = "registered"
}

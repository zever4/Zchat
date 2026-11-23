package chat

import (
	"context"
	"testing"
	"time"
)

func TestReceiptRouting(t *testing.T) {
	h := NewHub()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go h.Run(ctx)
	defer func() {
		h.Stop()
		if err := h.Wait(ctx); err != nil {
			t.Error(err)
		}
	}()
	a := &Client{ID: 1, sessionID: "a", Send: make(chan []byte, 8)}
	b := &Client{ID: 1, sessionID: "b", Send: make(chan []byte, 8)}
	read := func(c *Client) string {
		t.Helper()
		select {
		case data := <-c.Send:
			return string(data)
		case <-ctx.Done():
			t.Fatal("hub did not route message")
			return ""
		}
	}
	if !h.RegisterClient(ctx, a) || !h.RegisterClient(ctx, b) {
		t.Fatal("register")
	}
	read(a)
	read(b) // greetings
	h.Enqueue(ctx, &DirectMessage{Connection: a, Payload: []byte("receipt")})
	h.Enqueue(ctx, &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("marker")})
	if read(a) != "receipt" || read(a) != "marker" || read(b) != "marker" {
		t.Fatal("receipt reached another connection")
	}
	h.DisconnectSession(1, "a")
	h.Enqueue(ctx, &DirectMessage{Connection: a, Payload: []byte("late receipt")})
	h.Enqueue(ctx, &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("alive")})
	if read(b) != "alive" {
		t.Fatal("late receipt reached another client")
	}
}

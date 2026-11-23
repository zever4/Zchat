package chat

import (
	"context"
	"testing"
	"time"
)

func startTestHub(t *testing.T) *Hub {
	t.Helper()
	hub := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); hub.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("hub did not stop")
		}
	})
	return hub
}
func registerTestClient(t *testing.T, hub *Hub, id int64) *Client {
	t.Helper()
	client := &Client{ID: id, Hub: hub, Send: make(chan []byte, 1)}
	select {
	case hub.Register <- client:
	case <-time.After(time.Second):
		t.Fatal("registration blocked")
	}
	receiveTestPayload(t, client) // Greeting is a registration barrier.
	return client
}
func receiveTestPayload(t *testing.T, c *Client) []byte {
	t.Helper()
	select {
	case payload, open := <-c.Send:
		if !open {
			t.Fatal("connection unexpectedly removed")
		}
		return payload
	case <-time.After(time.Second):
		t.Fatal("missing delivery")
		return nil
	}
}
func TestHubDeliversToAllUserConnections(t *testing.T) {
	hub := startTestHub(t)
	first := registerTestClient(t, hub, 1)
	second := registerTestClient(t, hub, 1)
	other := registerTestClient(t, hub, 2)
	hub.Direct <- &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("both")}
	for _, c := range []*Client{first, second} {
		if string(receiveTestPayload(t, c)) != "both" {
			t.Fatal("wrong payload")
		}
	}
	select {
	case <-other.Send:
		t.Fatal("delivered to unrelated user")
	default:
	}
	hub.Unregister <- first
	select {
	case _, open := <-first.Send:
		if open {
			t.Fatal("removed connection left open")
		}
	case <-time.After(time.Second):
		t.Fatal("unregister blocked")
	}
	// Repeated unregister must not remove a sibling or close a channel twice.
	hub.Unregister <- first
	hub.Direct <- &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("survivor")}
	if string(receiveTestPayload(t, second)) != "survivor" {
		t.Fatal("sibling lost")
	}
}
func TestHubSlowTabDoesNotDisconnectOtherTabs(t *testing.T) {
	hub := startTestHub(t)
	slow := registerTestClient(t, hub, 1)
	healthy := registerTestClient(t, hub, 1)
	slow.Send <- []byte("full queue")
	hub.Direct <- &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("live")}
	if string(receiveTestPayload(t, healthy)) != "live" {
		t.Fatal("healthy tab lost")
	}
	receiveTestPayload(t, slow) // Drain the buffered item, then observe closure.
	select {
	case _, open := <-slow.Send:
		if open {
			t.Fatal("slow connection retained")
		}
	case <-time.After(time.Second):
		t.Fatal("slow connection not removed")
	}
	hub.Unregister <- slow
	hub.Direct <- &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("next")}
	if string(receiveTestPayload(t, healthy)) != "next" {
		t.Fatal("healthy tab lost after stale unregister")
	}
}

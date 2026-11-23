package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Opt-in against a disposable Redis, never an implicit localhost/default DB.
func TestPubSubRedis(t *testing.T) {
	addr := os.Getenv("ZCHAT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set ZCHAT_TEST_REDIS_ADDR for Redis integration test")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	publisher, subscriber := NewClient(addr), NewClient(addr)
	t.Cleanup(func() { publisher.Close(); subscriber.Close() })
	if err := publisher.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	channel := "zchat-test:" + uuid.NewString()
	sub := subscriber.Subscribe(ctx, channel)
	defer sub.Close()
	// Wait for the actual subscription acknowledgement before publishing.
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	payload := []byte{0, 1, 255, 'h', 'i', 'n', 't'}
	if err := publisher.Publish(ctx, channel, payload); err != nil {
		t.Fatal(err)
	}
	message, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Channel != channel || message.Payload != string(payload) {
		t.Fatal("Pub/Sub channel or binary payload changed")
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	if err := publisher.Ping(stopped); err == nil {
		t.Fatal("Redis ignored caller cancellation")
	}
	if err := publisher.Ping(ctx); err != nil {
		t.Fatal("Redis client unusable after caller cancellation", err)
	}
}

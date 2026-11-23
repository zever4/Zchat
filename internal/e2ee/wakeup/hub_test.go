package wakeup

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"
	"github.com/zever4/Zchat/internal/redis"
)

func runHub(t *testing.T, hub *Hub) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); hub.Run(ctx) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Fatal("hub shutdown blocked")
		}
	}
	t.Cleanup(stop)
	return stop
}

func TestHubBoundsCoalescingAndRouting(t *testing.T) {
	h := New(nil)
	generation := uuid.NewString()
	ch, release, ok := h.Subscribe(generation)
	if !ok {
		t.Fatal("subscription refused")
	}
	for range 10_000 {
		h.Notify(generation)
	}
	if len(ch) != 1 {
		t.Fatal("duplicate hints did not coalesce")
	}
	<-ch
	h.Notify(uuid.NewString())
	if len(ch) != 0 {
		t.Fatal("hint leaked across generations")
	}
	var releases []func()
	for range capacity - 1 {
		_, cancel, ok := h.Subscribe(uuid.NewString())
		if !ok {
			t.Fatal("early capacity refusal")
		}
		releases = append(releases, cancel)
	}
	if _, _, ok := h.Subscribe(generation); ok {
		t.Fatal("unbounded subscriptions")
	}
	release()
	release() // idempotent even during concurrent publication
	if _, cancel, ok := h.Subscribe(generation); !ok {
		t.Fatal("capacity leaked")
	} else {
		cancel()
	}
	for _, cancel := range releases {
		cancel()
	}
	if h.count != 0 || len(h.listeners) != 0 {
		t.Fatal("unsubscribed listeners retained")
	}
	if _, _, ok := h.Subscribe("invalid"); ok {
		t.Fatal("invalid generation")
	}
}

func TestHubUnavailableRedisDoesNotBlockOrLeak(t *testing.T) {
	h := New(redis.NewClient("127.0.0.1:1"))
	stop := runHub(t, h)
	generation := uuid.NewString()
	ch, release, _ := h.Subscribe(generation)
	defer release()
	start := time.Now()
	for range 10_000 {
		h.Notify(generation)
	}
	if time.Since(start) > time.Second || len(ch) != 1 || len(h.queue) > cap(h.queue) {
		t.Fatal("Redis failure blocked local hints")
	}
	stop()
}

func TestHubRedisCrossInstanceAndReconnect(t *testing.T) {
	addr := os.Getenv("ZCHAT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set ZCHAT_TEST_REDIS_ADDR to a disposable Redis")
	}
	a, b := New(redis.NewClient(addr)), New(redis.NewClient(addr))
	runHub(t, a)
	runHub(t, b)
	generation := uuid.NewString()
	ch, release, _ := b.Subscribe(generation)
	defer release()
	// Retry publication because hints sent before subscription ACK may be lost.
	converge := func() {
		t.Helper()
		deadline := time.NewTimer(8 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ch:
				return
			case <-tick.C:
				a.Notify(generation)
			case <-deadline.C:
				t.Fatal("Redis hint did not recover")
			}
		}
	}
	converge()
	admin := redislib.NewClient(&redislib.Options{Addr: addr})
	defer admin.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := admin.Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err(); err != nil {
		t.Fatal(err)
	}
	// A new generation cannot have a buffered pre-disconnect notification.
	generation = uuid.NewString()
	ch, release, _ = b.Subscribe(generation)
	defer release()
	converge()
}

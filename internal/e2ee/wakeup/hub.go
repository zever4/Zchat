// Package wakeup carries lossy hints, never messages, receipts or ACKs.
package wakeup

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"
	"github.com/zever4/Zchat/internal/redis"
)

const topic = "zchat:e2ee:inbox-hint:v1"
const capacity = 1024

// Hub coalesces hints per subscription. One bounded publication queue and two
// shared Redis loops replace per-message jobs. Redis is optional and Hub-owned.
type Hub struct {
	redis     *redis.Client
	queue     chan string
	mu        sync.Mutex
	listeners map[string]map[chan struct{}]struct{}
	count     int
}

func New(client *redis.Client) *Hub {
	return &Hub{redis: client, queue: make(chan string, 128), listeners: make(map[string]map[chan struct{}]struct{})}
}

func valid(generation string) bool {
	id, err := uuid.Parse(generation)
	return err == nil && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == generation
}

// Subscribe is called only after the RPC guard binds the current generation.
// Never close the channel: an in-flight notification may still reference it.
func (h *Hub) Subscribe(generation string) (<-chan struct{}, func(), bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !valid(generation) || h.count >= capacity {
		return nil, nil, false
	}
	ch := make(chan struct{}, 1)
	if h.listeners[generation] == nil {
		h.listeners[generation] = make(map[chan struct{}]struct{})
	}
	h.listeners[generation][ch] = struct{}{}
	h.count++
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(h.listeners[generation], ch)
			if len(h.listeners[generation]) == 0 {
				delete(h.listeners, generation)
			}
			h.count--
		})
	}, true
}

func (h *Hub) local(generation string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.listeners[generation] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Notify is nonblocking. Call only after a successful durable Send COMMIT.
// Exact retries can duplicate hints; full queues can lose them. Polling recovers.
func (h *Hub) Notify(generation string) {
	if !valid(generation) {
		return
	}
	h.local(generation)
	if h.redis != nil {
		select {
		case h.queue <- generation:
		default:
		}
	}
}

func (h *Hub) Run(ctx context.Context) {
	if h.redis == nil {
		<-ctx.Done()
		return
	}
	stop := context.AfterFunc(ctx, func() { h.redis.Close() })
	defer stop()
	defer h.redis.Close()
	var publishers sync.WaitGroup
	publishers.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case generation := <-h.queue:
				bounded, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
				// Best effort only. Failure never changes a committed Send result.
				_ = h.redis.Publish(bounded, topic, []byte(generation))
				cancel()
			}
		}
	})
	defer publishers.Wait()
	backoff := time.Second
	for ctx.Err() == nil {
		sub := h.redis.Subscribe(ctx, topic)
		closeOnStop := context.AfterFunc(ctx, func() { sub.Close() })
		for ctx.Err() == nil {
			frame, err := sub.Receive(ctx)
			if err != nil {
				break
			}
			// Subscription ACKs/PONGs carry no hint. Validate before routing.
			switch message := frame.(type) {
			case *redislib.Subscription:
				backoff = time.Second
			case *redislib.Message:
				if message.Channel == topic && valid(message.Payload) {
					h.local(message.Payload)
				}
			}
		}
		closeOnStop()
		sub.Close()
		timer := time.NewTimer(backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	client *redis.Client
}

func NewClient(addr string) *Client {
	return &Client{
		client: redis.NewClient(&redis.Options{
			Addr:                  addr,
			ContextTimeoutEnabled: true,
			DialTimeout:           3 * time.Second,
			ReadTimeout:           3 * time.Second,
			WriteTimeout:          3 * time.Second,
		}),
	}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Client) Publish(ctx context.Context, channel string, payload []byte) error {
	return c.client.Publish(ctx, channel, payload).Err()
}

func (c *Client) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return c.client.Subscribe(ctx, channel)
}

func (c *Client) Close() error {
	return c.client.Close()
}

package coupon

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisValidator validates codes against a Redis SET holding the precomputed
// valid-code set (seeded by cmd/seedredis from a built index — never the raw
// corpus). It exists to demonstrate the Validator seam: the swap-in point for
// the day coupon state becomes mutable and shared.
//
// Failure policy: if Redis cannot answer, Validate returns an error and the
// caller responds 503 — fail closed. An unreachable Redis must never approve
// a discount, and silently rejecting valid coupons would be just as wrong.
type RedisValidator struct {
	client *redis.Client
	key    string
}

// NewRedisValidator connects and verifies the seeded set exists.
func NewRedisValidator(addr, key string) (*RedisValidator, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis validator: ping %s: %w", addr, err)
	}
	n, err := client.SCard(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis validator: SCARD %s: %w", key, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("redis validator: set %q is empty — run cmd/seedredis first", key)
	}
	return &RedisValidator{client: client, key: key}, nil
}

func (v *RedisValidator) Validate(ctx context.Context, raw string) (bool, error) {
	code, ok := Normalize(raw)
	if !ok {
		return false, nil // shape failures never need the network
	}
	hit, err := v.client.SIsMember(ctx, v.key, code).Result()
	if err != nil {
		return false, fmt.Errorf("redis validator: %w", err)
	}
	return hit, nil
}

func (v *RedisValidator) Info() map[string]any {
	return map[string]any{"mode": "redis", "addr": v.client.Options().Addr, "key": v.key}
}

func (v *RedisValidator) Healthy(ctx context.Context) error {
	return v.client.Ping(ctx).Err()
}

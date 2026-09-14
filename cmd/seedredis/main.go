// Command seedredis loads a built coupon index into a Redis SET, for running
// the API with VALIDATOR=redis. It seeds the ANSWER (the tiny valid set),
// never the raw corpus — see docs/DESIGN.md ("How would you do it with Redis?").
//
// The set is written to a versioned key and atomically RENAMEd into place, so
// a live server never observes a half-seeded set.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/SaiPavan-GGNEXT/gokart/internal/coupon"
)

func main() {
	idxPath := flag.String("index", "data/coupons.idx", "coupon index to seed from")
	addr := flag.String("addr", "localhost:6379", "redis address")
	key := flag.String("key", "coupons:valid", "destination set key")
	flag.Parse()

	idx, err := coupon.LoadIndex(*idxPath)
	if err != nil {
		log.Fatalf("seedredis: %v", err)
	}
	codes := idx.Codes()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: *addr})
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("seedredis: ping %s: %v", *addr, err)
	}

	staging := *key + ":staging:" + uuid.NewString()
	members := make([]any, len(codes))
	for i, c := range codes {
		members[i] = c
	}
	if err := client.SAdd(ctx, staging, members...).Err(); err != nil {
		log.Fatalf("seedredis: SADD: %v", err)
	}
	if err := client.Rename(ctx, staging, *key).Err(); err != nil {
		log.Fatalf("seedredis: RENAME: %v", err)
	}
	fmt.Printf("seeded %d codes into %s at %s (index built %s)\n",
		len(codes), *key, *addr, idx.Meta.BuiltAt.Format(time.RFC3339))
}

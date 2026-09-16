// Package config loads all runtime configuration from environment variables
// with safe defaults, so a bare `go run ./cmd/server` works out of the box.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Scopes recognized by the API. Keys are mapped to scopes via API_KEYS.
const (
	ScopeCreateOrder    = "create_order"
	ScopeManageProducts = "manage_products"
)

// Config is the fully resolved server configuration.
type Config struct {
	Port        string
	Store       string // "memory" | "postgres"
	DatabaseURL string // primary: all writes + schema
	// DatabaseReplicaURL: optional read replica(s), comma-separated. Catalog
	// reads round-robin over these; writes and order transactions always use
	// the primary. Empty = single-node (reads share the primary pool).
	DatabaseReplicaURL string
	// CatalogRefreshInterval: with STORE=postgres, products are served from an
	// in-memory snapshot refreshed on this cadence (0 disables the cache and
	// reads hit the database per request).
	CatalogRefreshInterval time.Duration
	Validator              string // "index" | "redis" | "static" (static is test-only and must be explicit)
	CouponIndexPath        string // local path, or http(s):// URL (e.g. an S3 object)
	// CouponReloadInterval, when >0 and CouponIndexPath is a URL, polls the
	// URL and hot-swaps the index on change — live corpus updates without
	// restarts. 0 disables polling (fetch once at startup).
	CouponReloadInterval time.Duration
	RedisAddr            string
	RedisKey             string // set name holding valid codes
	APIKeys              map[string][]string
	SeedProducts         bool // seed catalog at startup when the store is empty
	BodyLimitBytes       int
	RateLimitRPM         int    // per-IP requests/minute; 0 disables
	CORSOrigins          string // comma-separated allowed origins; "*" for any
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	IdleTimeout          time.Duration
	ShutdownTimeout      time.Duration
	Env                  string // "dev" | "prod"
}

// defaultAPIKeys grants the spec's documented key the scopes it needs, plus a
// deliberately scope-less key so 403 (valid key, missing scope) is demonstrable.
const defaultAPIKeys = `{"apitest":["create_order","manage_products"],"apitest_noscope":[]}`

// Load reads configuration from the environment.
func Load() (*Config, error) {
	c := &Config{
		Port:                   getenv("PORT", "8080"),
		Store:                  getenv("STORE", "memory"),
		DatabaseURL:            os.Getenv("DATABASE_URL"),
		DatabaseReplicaURL:     os.Getenv("DATABASE_REPLICA_URL"),
		CatalogRefreshInterval: getenvDur("CATALOG_REFRESH_INTERVAL", 2*time.Minute),
		Validator:              getenv("VALIDATOR", "index"),
		CouponIndexPath:        getenv("COUPON_INDEX", "data/coupons.idx"),
		CouponReloadInterval:   getenvDur("COUPON_RELOAD_INTERVAL", 0),
		RedisAddr:              getenv("REDIS_ADDR", "localhost:6379"),
		RedisKey:               getenv("REDIS_KEY", "coupons:valid"),
		SeedProducts:           getenvBool("SEED_PRODUCTS", true),
		BodyLimitBytes:         getenvInt("BODY_LIMIT_BYTES", 1<<20), // 1 MiB
		RateLimitRPM:           getenvInt("RATE_LIMIT_RPM", 300),
		CORSOrigins:            getenv("CORS_ORIGINS", "*"),
		ReadTimeout:            getenvDur("READ_TIMEOUT", 10*time.Second),
		WriteTimeout:           getenvDur("WRITE_TIMEOUT", 10*time.Second),
		IdleTimeout:            getenvDur("IDLE_TIMEOUT", 60*time.Second),
		ShutdownTimeout:        getenvDur("SHUTDOWN_TIMEOUT", 10*time.Second),
		Env:                    getenv("ENV", "dev"),
	}

	raw := getenv("API_KEYS", defaultAPIKeys)
	if err := json.Unmarshal([]byte(raw), &c.APIKeys); err != nil {
		return nil, fmt.Errorf("API_KEYS must be JSON of {key: [scopes]}: %w", err)
	}
	if len(c.APIKeys) == 0 {
		return nil, fmt.Errorf("API_KEYS must define at least one key")
	}

	switch c.Store {
	case "memory":
	case "postgres":
		if c.DatabaseURL == "" {
			return nil, fmt.Errorf("STORE=postgres requires DATABASE_URL")
		}
	default:
		return nil, fmt.Errorf("unknown STORE %q (memory|postgres)", c.Store)
	}

	switch c.Validator {
	case "index", "redis":
	case "static":
		// Explicitly requested test stub; main() decides whether to allow it.
	default:
		return nil, fmt.Errorf("unknown VALIDATOR %q (index|redis)", c.Validator)
	}
	return c, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func getenvDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

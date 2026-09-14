// Command server runs the food-ordering API.
//
// Startup is fail-fast by design: if the coupon index (or any configured
// backend) is missing or corrupt, the process exits non-zero with an
// actionable message. A server that cannot answer coupon questions
// truthfully must not serve traffic — see docs/DESIGN.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saipavankumar/kart-challenge/internal/api"
	"github.com/saipavankumar/kart-challenge/internal/config"
	"github.com/saipavankumar/kart-challenge/internal/coupon"
	"github.com/saipavankumar/kart-challenge/internal/domain"
	"github.com/saipavankumar/kart-challenge/internal/seed"
	"github.com/saipavankumar/kart-challenge/internal/service"
	"github.com/saipavankumar/kart-challenge/internal/store"
	"github.com/saipavankumar/kart-challenge/internal/store/memory"
	"github.com/saipavankumar/kart-challenge/internal/store/postgres"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false,
		"probe the local /healthz endpoint and exit (for container HEALTHCHECK)")
	flag.Parse()

	if *healthcheck {
		os.Exit(runHealthcheck())
	}
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	productStore, orderStore, closeStores, err := buildStores(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeStores()

	validator, err := buildValidator(cfg)
	if err != nil {
		return err
	}
	slog.Info("coupon validator ready", "info", validator.Info())

	if cfg.SeedProducts {
		if err := seedProducts(ctx, productStore); err != nil {
			return fmt.Errorf("seeding products: %w", err)
		}
	}

	deps := api.Deps{
		Products: service.NewProducts(productStore),
		Orders:   service.NewOrders(productStore, orderStore, validator),
		Ready: func(ctx context.Context) (map[string]any, error) {
			if err := productStore.Healthy(ctx); err != nil {
				return nil, fmt.Errorf("product store: %w", err)
			}
			if err := orderStore.Healthy(ctx); err != nil {
				return nil, fmt.Errorf("order store: %w", err)
			}
			if err := validator.Healthy(ctx); err != nil {
				return nil, fmt.Errorf("coupon validator: %w", err)
			}
			return map[string]any{
				"store":     cfg.Store,
				"validator": validator.Info(),
			}, nil
		},
	}
	app := api.New(cfg, deps)

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "port", cfg.Port, "store", cfg.Store, "validator", cfg.Validator)
		errCh <- app.Listen(":" + cfg.Port)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received, draining in-flight requests",
			"timeout", cfg.ShutdownTimeout.String())
		if err := app.ShutdownWithTimeout(cfg.ShutdownTimeout); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		slog.Info("shutdown complete")
		return nil
	}
}

// productSeeder is the optional store capability used for fixed-id seed rows.
type productSeeder interface {
	CreateWithID(ctx context.Context, id string, np domain.NewProduct) (*domain.Product, error)
}

func buildStores(ctx context.Context, cfg *config.Config) (store.ProductStore, store.OrderStore, func(), error) {
	switch cfg.Store {
	case "postgres":
		pg, err := postgres.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"connecting to postgres (set STORE=memory to run without a database): %w", err)
		}
		slog.Info("store ready", "store", "postgres")
		return pg, pg.Orders(), pg.Close, nil
	default: // "memory" — validated by config.Load
		slog.Info("store ready", "store", "memory",
			"note", "orders do not survive restarts; STORE=postgres for durability")
		return memory.NewProductStore(), memory.NewOrderStore(), func() {}, nil
	}
}

func buildValidator(cfg *config.Config) (coupon.Validator, error) {
	switch cfg.Validator {
	case "redis":
		v, err := coupon.NewRedisValidator(cfg.RedisAddr, cfg.RedisKey)
		if err != nil {
			return nil, fmt.Errorf("VALIDATOR=redis: %w", err)
		}
		return v, nil
	case "static":
		// Test fixture only; refuse outside dev so a missing index can never
		// be silently papered over in a real environment.
		if cfg.Env != "dev" {
			return nil, errors.New("VALIDATOR=static is only allowed with ENV=dev (it is a test fixture)")
		}
		slog.Warn("USING STATIC TEST VALIDATOR — coupon answers are fixtures, not real corpus data")
		return coupon.NewStaticValidator("HAPPYHRS", "FIFTYOFF")
	default: // "index"
		v, err := coupon.NewIndexValidator(cfg.CouponIndexPath)
		if err != nil {
			return nil, fmt.Errorf(
				"coupon index unavailable at %q — build it with `make index` "+
					"(or `go run ./cmd/indexer -out %s data/couponbase*.gz`): %w",
				cfg.CouponIndexPath, cfg.CouponIndexPath, err)
		}
		return v, nil
	}
}

func seedProducts(ctx context.Context, ps store.ProductStore) error {
	n, err := ps.Count(ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		slog.Info("catalog already populated, skipping seed", "products", n)
		return nil
	}
	s, ok := ps.(productSeeder)
	if !ok {
		return errors.New("store does not support seeding")
	}
	rows, err := seed.Products()
	if err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := s.CreateWithID(ctx, r.ID, r.NewProduct); err != nil && !errors.Is(err, store.ErrConflict) {
			return fmt.Errorf("seeding %q: %w", r.Name, err)
		}
	}
	slog.Info("seeded product catalog", "products", len(rows))
	return nil
}

func runHealthcheck() int {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

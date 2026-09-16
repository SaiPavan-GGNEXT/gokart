// Package cached decorates a ProductStore with a refresh-ahead snapshot:
// reads are served from an immutable in-memory snapshot (zero I/O, lock-free
// via atomic.Pointer), a background ticker rebuilds it from the inner store,
// and writes go through to the inner store AND update the local snapshot
// immediately (read-your-writes on the writing instance).
//
// This is the third instance of the repo's one data pattern — immutable
// snapshot + background refresh + atomic swap + fail-static on error — after
// the coupon index (RemoteValidator) and the corpus watcher. The database
// drops out of the read hot path entirely: with N instances the read load on
// the store is N cheap checks per interval, independent of traffic.
//
// Consistency: staleness is bounded by the refresh interval (menu-appropriate;
// a product created on another instance appears here within one tick). The
// order path is unaffected — it resolves products through this same snapshot
// (GetMany), which is exactly as fresh as the menu the customer just saw.
package cached

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
	"github.com/SaiPavan-GGNEXT/gokart/internal/store"
)

// fingerprinter is the optional cheap change-detector (postgres implements it:
// count+max(id)). Without it, every tick does a full reload — still cheap.
type fingerprinter interface {
	Fingerprint(ctx context.Context) (string, error)
}

// idCreator is the optional fixed-id seeding capability, forwarded so startup
// seeding works through the decorator.
type idCreator interface {
	CreateWithID(ctx context.Context, id string, np domain.NewProduct) (*domain.Product, error)
}

type snapshot struct {
	list     []domain.Product          // insertion/id order; treated as immutable
	byID     map[string]domain.Product // id → product
	loadedAt time.Time                 // last FULL reload from the inner store
	fp       string                    // fingerprint at load time, "" if unsupported
}

// ProductStore is the caching decorator. Satisfies store.ProductStore.
type ProductStore struct {
	inner    store.ProductStore
	interval time.Duration
	snap     atomic.Pointer[snapshot]
}

// initialLoadBudget bounds boot-time retries. The retry exists because the
// first load can race replication on a cold standby: the schema is applied
// on the PRIMARY milliseconds before the snapshot reads from the REPLICA,
// which may not have replayed the DDL yet ("relation does not exist").
// Observed live in the compose replica profile — not hypothetical.
const initialLoadBudget = 30 * time.Second

// New loads the initial snapshot (fail-fast after the retry budget: a store
// we cannot read at boot is a store we must not pretend to serve) and starts
// the refresh loop.
func New(ctx context.Context, inner store.ProductStore, interval time.Duration) (*ProductStore, error) {
	c := &ProductStore{inner: inner, interval: interval}

	deadline := time.Now().Add(initialLoadBudget)
	for {
		err := c.Refresh(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("catalog cache: initial load: %w", err)
		}
		slog.Warn("catalog cache: initial load failed; retrying (replica may still be catching up)",
			"error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	if interval > 0 {
		go c.loop(ctx)
	}
	return c, nil
}

func (c *ProductStore) loop(ctx context.Context) {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Refresh(ctx); err != nil {
				// Fail-static: the previous snapshot keeps serving.
				slog.Warn("catalog cache: refresh failed; serving previous snapshot",
					"age", time.Since(c.snap.Load().loadedAt).Round(time.Second).String(),
					"error", err)
			}
		}
	}
}

// Refresh rebuilds the snapshot from the inner store, skipping the full
// reload when the fingerprint says nothing changed.
func (c *ProductStore) Refresh(ctx context.Context) error {
	var fp string
	if f, ok := c.inner.(fingerprinter); ok {
		got, err := f.Fingerprint(ctx)
		if err != nil {
			return fmt.Errorf("fingerprint: %w", err)
		}
		if cur := c.snap.Load(); cur != nil && cur.fp == got {
			return nil // unchanged — skip the full scan
		}
		fp = got
	}

	list, err := c.inner.List(ctx)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	byID := make(map[string]domain.Product, len(list))
	for _, p := range list {
		byID[p.ID] = p
	}
	c.snap.Store(&snapshot{list: list, byID: byID, loadedAt: time.Now().UTC(), fp: fp})
	return nil
}

// --- reads: pure snapshot, zero I/O ----------------------------------------

func (c *ProductStore) List(context.Context) ([]domain.Product, error) {
	return c.snap.Load().list, nil // immutable by convention; rebuilt, never mutated
}

func (c *ProductStore) Get(_ context.Context, id string) (*domain.Product, error) {
	p, ok := c.snap.Load().byID[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &p, nil
}

func (c *ProductStore) GetMany(_ context.Context, ids []string) (map[string]domain.Product, error) {
	snap := c.snap.Load()
	out := make(map[string]domain.Product, len(ids))
	for _, id := range ids {
		if p, ok := snap.byID[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

func (c *ProductStore) Count(context.Context) (int, error) {
	return len(c.snap.Load().list), nil
}

// --- writes: through to the store, then into the snapshot -------------------

func (c *ProductStore) Create(ctx context.Context, np domain.NewProduct) (*domain.Product, error) {
	p, err := c.inner.Create(ctx, np)
	if err != nil {
		return nil, err
	}
	c.add(*p)
	return p, nil
}

// CreateWithID forwards fixed-id seeding when the inner store supports it.
func (c *ProductStore) CreateWithID(ctx context.Context, id string, np domain.NewProduct) (*domain.Product, error) {
	ic, ok := c.inner.(idCreator)
	if !ok {
		return nil, fmt.Errorf("catalog cache: inner store does not support seeding")
	}
	p, err := ic.CreateWithID(ctx, id, np)
	if err != nil {
		return nil, err
	}
	c.add(*p)
	return p, nil
}

// add applies copy-on-write: never mutate a published snapshot. The
// fingerprint is cleared so the next tick reconciles with the store.
func (c *ProductStore) add(p domain.Product) {
	for {
		old := c.snap.Load()
		list := make([]domain.Product, len(old.list), len(old.list)+1)
		copy(list, old.list)
		list = append(list, p)
		byID := make(map[string]domain.Product, len(old.byID)+1)
		for k, v := range old.byID {
			byID[k] = v
		}
		byID[p.ID] = p
		if c.snap.CompareAndSwap(old, &snapshot{
			list: list, byID: byID, loadedAt: old.loadedAt, fp: "",
		}) {
			return
		}
	}
}

// --- health & observability --------------------------------------------------

// Healthy reflects the fail-static contract: a loaded snapshot can serve
// reads even during a store outage (which still surfaces via the order
// store's health on the same pools).
func (c *ProductStore) Healthy(context.Context) error {
	if c.snap.Load() == nil {
		return fmt.Errorf("catalog snapshot not loaded")
	}
	return nil
}

// SnapshotInfo is surfaced on /readyz: "how fresh is this instance's menu?"
func (c *ProductStore) SnapshotInfo() map[string]any {
	s := c.snap.Load()
	return map[string]any{
		"mode":             "snapshot-cache",
		"products":         len(s.list),
		"loaded_at":        s.loadedAt,
		"age":              time.Since(s.loadedAt).Round(time.Second).String(),
		"refresh_interval": c.interval.String(),
	}
}

// Package postgres implements the store interfaces on PostgreSQL via pgx.
// Orders are written transactionally (order + items commit atomically);
// product resolution is batched (one query, no N+1).
//
// Read/write split: writes always target the primary (DATABASE_URL); reads
// go to read replicas (DATABASE_REPLICA_URL, comma-separated, round-robin)
// when configured, protecting the primary from all read pressure. With no
// replica configured, both roles share one pool — zero behavior change.
// Replication is async, so replica reads are eventually consistent: fine
// for the catalog (staleness-tolerant), which is why ORDER writes and their
// transactional reads never touch replicas.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
	"github.com/SaiPavan-GGNEXT/gokart/internal/store"
)

//go:embed schema.sql
var schemaSQL string

// Store bundles both interfaces over a primary pool and optional replicas.
type Store struct {
	write *pgxpool.Pool   // primary: all writes, schema, transactions
	reads []*pgxpool.Pool // replicas: catalog reads; empty = use primary
	rr    atomic.Uint64   // round-robin cursor over reads
}

// Connect opens the primary pool (verifying connectivity and applying the
// schema there — replicas are read-only) plus a pool per replica URL.
func Connect(ctx context.Context, databaseURL, replicaURLs string) (*Store, error) {
	write, err := newPool(ctx, databaseURL, "DATABASE_URL")
	if err != nil {
		return nil, err
	}
	if _, err := write.Exec(ctx, schemaSQL); err != nil {
		write.Close()
		return nil, fmt.Errorf("postgres: applying schema: %w", err)
	}

	s := &Store{write: write}
	for _, u := range strings.Split(replicaURLs, ",") {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		r, err := newPool(ctx, u, "DATABASE_REPLICA_URL")
		if err != nil {
			s.Close()
			return nil, err
		}
		s.reads = append(s.reads, r)
	}
	return s, nil
}

func newPool(ctx context.Context, url, label string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse %s: %w", label, err)
	}
	cfg.MaxConns = 10
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: pool (%s): %w", label, err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping (%s): %w", label, err)
	}
	return pool, nil
}

// readPool picks a replica round-robin, or the primary when none exist.
func (s *Store) readPool() *pgxpool.Pool {
	if len(s.reads) == 0 {
		return s.write
	}
	return s.reads[int(s.rr.Add(1))%len(s.reads)]
}

func (s *Store) Close() {
	s.write.Close()
	for _, r := range s.reads {
		r.Close()
	}
}

func (s *Store) Healthy(ctx context.Context) error {
	if err := s.write.Ping(ctx); err != nil {
		return fmt.Errorf("primary: %w", err)
	}
	for i, r := range s.reads {
		if err := r.Ping(ctx); err != nil {
			return fmt.Errorf("replica %d: %w", i, err)
		}
	}
	return nil
}

// Fingerprint is a cheap change-detector for the catalog (used by the
// snapshot cache to skip full reloads): the table is insert-only, so
// count + max id identify its state. Reads from a replica.
func (s *Store) Fingerprint(ctx context.Context) (string, error) {
	var count, maxID int64
	err := s.readPool().QueryRow(ctx,
		`SELECT count(*), coalesce(max(id), 0) FROM products`).Scan(&count, &maxID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d|%d", count, maxID), nil
}

// Topology reports the pool layout for /readyz.
func (s *Store) Topology() map[string]any {
	return map[string]any{"primary": true, "read_replicas": len(s.reads)}
}

// --- ProductStore ---------------------------------------------------------

func (s *Store) List(ctx context.Context) ([]domain.Product, error) {
	rows, err := s.readPool().Query(ctx,
		`SELECT id, name, price, category FROM products ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Product{}
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) Get(ctx context.Context, id string) (*domain.Product, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, store.ErrNotFound // non-numeric ids cannot exist here
	}
	row := s.readPool().QueryRow(ctx,
		`SELECT id, name, price, category FROM products WHERE id = $1`, n)
	p, err := scanProduct(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) GetMany(ctx context.Context, ids []string) (map[string]domain.Product, error) {
	nums := make([]int64, 0, len(ids))
	for _, id := range ids {
		if n, err := strconv.ParseInt(id, 10, 64); err == nil {
			nums = append(nums, n)
		}
	}
	out := make(map[string]domain.Product, len(nums))
	if len(nums) == 0 {
		return out, nil
	}
	rows, err := s.readPool().Query(ctx,
		`SELECT id, name, price, category FROM products WHERE id = ANY($1)`, nums)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		p, err := scanProduct(rows)
		if err != nil {
			return nil, err
		}
		out[p.ID] = p
	}
	return out, rows.Err()
}

func (s *Store) Create(ctx context.Context, np domain.NewProduct) (*domain.Product, error) {
	row := s.write.QueryRow(ctx,
		`INSERT INTO products (name, price, category) VALUES ($1, $2, $3)
		 RETURNING id, name, price, category`, np.Name, np.Price, np.Category)
	p, err := scanProduct(row)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// CreateWithID inserts seed rows under fixed ids, skipping existing ones.
func (s *Store) CreateWithID(ctx context.Context, id string, np domain.NewProduct) (*domain.Product, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("postgres: seed id %q is not numeric", id)
	}
	tag, err := s.write.Exec(ctx,
		`INSERT INTO products (id, name, price, category) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO NOTHING`, n, np.Name, np.Price, np.Category)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, store.ErrConflict
	}
	// Keep the identity sequence ahead of explicitly seeded ids.
	_, err = s.write.Exec(ctx,
		`SELECT setval(pg_get_serial_sequence('products','id'),
		        GREATEST((SELECT MAX(id) FROM products), 1))`)
	if err != nil {
		return nil, err
	}
	p := domain.Product{ID: id, Name: np.Name, Price: np.Price, Category: np.Category}
	return &p, nil
}

func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	err := s.readPool().QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&n)
	return n, err
}

// --- OrderStore -------------------------------------------------------------

// Orders returns a view of the store satisfying store.OrderStore
// (the Store itself satisfies store.ProductStore directly).
func (s *Store) Orders() store.OrderStore { return orderView{s} }

type orderView struct{ s *Store }

func (v orderView) Create(ctx context.Context, o *domain.Order) error {
	return v.s.CreateOrder(ctx, o)
}

func (v orderView) Healthy(ctx context.Context) error { return v.s.Healthy(ctx) }

func (s *Store) CreateOrder(ctx context.Context, o *domain.Order) error {
	tx, err := s.write.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after commit

	var coupon *string
	if o.CouponCode != "" {
		coupon = &o.CouponCode
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO orders (id, coupon_code) VALUES ($1, $2)`, o.ID, coupon); err != nil {
		return err
	}
	for i, item := range o.Items {
		pid, err := strconv.ParseInt(item.ProductID, 10, 64)
		if err != nil {
			return fmt.Errorf("order item %d: bad product id %q", i, item.ProductID)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO order_items (order_id, position, product_id, quantity)
			 VALUES ($1, $2, $3, $4)`, o.ID, i, pid, item.Quantity); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func scanProduct(row pgx.Row) (domain.Product, error) {
	var (
		id int64
		p  domain.Product
	)
	if err := row.Scan(&id, &p.Name, &p.Price, &p.Category); err != nil {
		return p, err
	}
	p.ID = strconv.FormatInt(id, 10)
	return p, nil
}

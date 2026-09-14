// Package postgres implements the store interfaces on PostgreSQL via pgx.
// Orders are written transactionally (order + items commit atomically);
// product resolution is batched (one query, no N+1).
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
	"github.com/SaiPavan-GGNEXT/gokart/internal/store"
)

//go:embed schema.sql
var schemaSQL string

// Store bundles both interfaces over one connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Connect opens the pool, verifies connectivity, and applies the schema.
func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: applying schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Healthy(ctx context.Context) error { return s.pool.Ping(ctx) }

// --- ProductStore ---------------------------------------------------------

func (s *Store) List(ctx context.Context) ([]domain.Product, error) {
	rows, err := s.pool.Query(ctx,
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
	row := s.pool.QueryRow(ctx,
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
	rows, err := s.pool.Query(ctx,
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
	row := s.pool.QueryRow(ctx,
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
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO products (id, name, price, category) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO NOTHING`, n, np.Name, np.Price, np.Category)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, store.ErrConflict
	}
	// Keep the identity sequence ahead of explicitly seeded ids.
	_, err = s.pool.Exec(ctx,
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
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&n)
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
	tx, err := s.pool.Begin(ctx)
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

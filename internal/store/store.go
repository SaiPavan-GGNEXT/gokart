// Package store defines the persistence interfaces. Implementations live in
// subpackages (memory, postgres); swapping one for the other is a config
// change, never a code change — the extensibility seam the API layer builds on.
package store

import (
	"context"
	"errors"

	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/domain"
)

// ErrNotFound is returned when a requested entity does not exist.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when an insert collides with an existing entity.
var ErrConflict = errors.New("already exists")

// ProductStore persists the product catalog.
type ProductStore interface {
	List(ctx context.Context) ([]domain.Product, error)
	// Get returns ErrNotFound when the id is unknown. The id is the
	// canonical decimal string form ("10").
	Get(ctx context.Context, id string) (*domain.Product, error)
	// GetMany resolves several ids in one round trip; missing ids are simply
	// absent from the result map (callers decide how to report them).
	GetMany(ctx context.Context, ids []string) (map[string]domain.Product, error)
	// Create assigns the id and returns the stored product.
	Create(ctx context.Context, p domain.NewProduct) (*domain.Product, error)
	// Count reports catalog size (used to decide whether to seed).
	Count(ctx context.Context) (int, error)
	Healthy(ctx context.Context) error
}

// OrderStore persists placed orders.
type OrderStore interface {
	Create(ctx context.Context, o *domain.Order) error
	Healthy(ctx context.Context) error
}

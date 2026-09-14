// Package memory provides thread-safe in-memory stores. This is the default
// backend: the assignment defines no persistence requirement, so the simplest
// correct store wins. STORE=postgres swaps in the durable implementation.
package memory

import (
	"context"
	"strconv"
	"sync"

	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/domain"
	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/store"
)

// ProductStore is an in-memory, mutex-guarded product catalog.
type ProductStore struct {
	mu     sync.RWMutex
	byID   map[string]domain.Product
	order  []string // insertion order, for stable List output
	nextID int64
}

func NewProductStore() *ProductStore {
	return &ProductStore{byID: map[string]domain.Product{}, nextID: 1}
}

func (s *ProductStore) List(context.Context) ([]domain.Product, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.Product, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.byID[id])
	}
	return out, nil
}

func (s *ProductStore) Get(_ context.Context, id string) (*domain.Product, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.byID[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &p, nil
}

func (s *ProductStore) GetMany(_ context.Context, ids []string) (map[string]domain.Product, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]domain.Product, len(ids))
	for _, id := range ids {
		if p, ok := s.byID[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

func (s *ProductStore) Create(_ context.Context, np domain.NewProduct) (*domain.Product, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := strconv.FormatInt(s.nextID, 10)
	s.nextID++
	p := domain.Product{ID: id, Name: np.Name, Price: np.Price, Category: np.Category}
	s.byID[id] = p
	s.order = append(s.order, id)
	return &p, nil
}

// CreateWithID inserts a product under a caller-chosen id (seed data keeps
// the spec's example: id 10 = Chicken Waffle). The auto-increment counter is
// advanced past explicit ids so later Creates never collide.
func (s *ProductStore) CreateWithID(_ context.Context, id string, np domain.NewProduct) (*domain.Product, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byID[id]; exists {
		return nil, store.ErrConflict
	}
	p := domain.Product{ID: id, Name: np.Name, Price: np.Price, Category: np.Category}
	s.byID[id] = p
	s.order = append(s.order, id)
	if n, err := strconv.ParseInt(id, 10, 64); err == nil && n >= s.nextID {
		s.nextID = n + 1
	}
	return &p, nil
}

func (s *ProductStore) Count(context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID), nil
}

func (s *ProductStore) Healthy(context.Context) error { return nil }

// OrderStore is an in-memory, mutex-guarded order log.
type OrderStore struct {
	mu     sync.RWMutex
	orders map[string]domain.Order
}

func NewOrderStore() *OrderStore {
	return &OrderStore{orders: map[string]domain.Order{}}
}

func (s *OrderStore) Create(_ context.Context, o *domain.Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders[o.ID] = *o
	return nil
}

// Len is used by tests to assert persistence.
func (s *OrderStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.orders)
}

func (s *OrderStore) Healthy(context.Context) error { return nil }

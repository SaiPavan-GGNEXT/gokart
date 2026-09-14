// Package service holds the business logic between HTTP handlers and stores.
// Handlers translate transport concerns; services decide what is valid.
package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/coupon"
	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/domain"
	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/store"
)

// Business limits. Structural bounds (item count, name lengths) guard against
// abuse; they are documented in the README as deliberate robustness choices.
const (
	MaxOrderItems = 100
	MaxQuantity   = 1_000_000
	MaxNameLen    = 200
	MaxCategory   = 100
	MaxPrice      = 1_000_000
)

// OrderRequest is the decoded POST /order body after transport validation.
// CouponCode: nil = not supplied; non-nil (even empty) = supplied and must
// be a valid promo code.
type OrderRequest struct {
	CouponCode *string
	Items      []domain.OrderItem
}

// Orders places orders: item validation, coupon validation, persistence.
type Orders struct {
	products  store.ProductStore
	orders    store.OrderStore
	validator coupon.Validator
}

func NewOrders(p store.ProductStore, o store.OrderStore, v coupon.Validator) *Orders {
	return &Orders{products: p, orders: o, validator: v}
}

// Place validates the request and persists the order.
//
// Error mapping (documented in README):
//   - structural problems were already rejected by the handler with 400
//   - semantic problems (unknown product, bad quantity, invalid coupon) → 422
//   - validator backend unavailable → 503 (fail closed: never guess about money)
func (s *Orders) Place(ctx context.Context, req OrderRequest) (*domain.Order, error) {
	if len(req.Items) == 0 {
		return nil, domain.ErrBadRequest("items is required and must not be empty")
	}
	if len(req.Items) > MaxOrderItems {
		return nil, domain.ErrValidation(fmt.Sprintf("too many items: max %d per order", MaxOrderItems))
	}

	// Validate the coupon FIRST: if a discount code is bad, reject before any
	// other work, and never leak whether the products existed.
	couponCode := ""
	if req.CouponCode != nil {
		valid, err := s.validator.Validate(ctx, *req.CouponCode)
		if err != nil {
			return nil, domain.ErrUnavailable("coupon validation is temporarily unavailable, please retry")
		}
		if !valid {
			return nil, domain.ErrValidation("invalid promo code")
		}
		couponCode, _ = coupon.Normalize(*req.CouponCode)
	}

	// Per-item semantic checks, collecting every problem (a client fixing a
	// request should learn all defects at once, not one per attempt).
	var problems []string
	ids := make([]string, 0, len(req.Items))
	for i, item := range req.Items {
		if strings.TrimSpace(item.ProductID) == "" {
			problems = append(problems, fmt.Sprintf("items[%d].productId is required", i))
			continue
		}
		if item.Quantity < 1 {
			problems = append(problems, fmt.Sprintf("items[%d].quantity must be at least 1", i))
		}
		if item.Quantity > MaxQuantity {
			problems = append(problems, fmt.Sprintf("items[%d].quantity exceeds max %d", i, MaxQuantity))
		}
		ids = append(ids, item.ProductID)
	}
	if len(problems) > 0 {
		return nil, domain.ErrValidation(strings.Join(problems, "; "))
	}

	// One batched lookup — no N+1, regardless of order size.
	found, err := s.products.GetMany(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("resolving products: %w", err)
	}
	var missing []string
	seen := map[string]bool{}
	for _, id := range ids {
		if _, ok := found[id]; !ok && !seen[id] {
			missing = append(missing, id)
			seen[id] = true
		}
	}
	if len(missing) > 0 {
		return nil, domain.ErrValidation("unknown productId(s): " + strings.Join(missing, ", "))
	}

	// Expand unique products in deterministic order for the response.
	uniq := make([]domain.Product, 0, len(found))
	added := map[string]bool{}
	for _, id := range ids {
		if !added[id] {
			uniq = append(uniq, found[id])
			added[id] = true
		}
	}

	order := &domain.Order{
		ID:         uuid.NewString(),
		Items:      req.Items,
		Products:   uniq,
		CouponCode: couponCode,
	}
	if err := s.orders.Create(ctx, order); err != nil {
		return nil, fmt.Errorf("persisting order: %w", err)
	}
	return order, nil
}

// Products wraps catalog reads/writes with validation.
type Products struct {
	store store.ProductStore
}

func NewProducts(p store.ProductStore) *Products { return &Products{store: p} }

func (s *Products) List(ctx context.Context) ([]domain.Product, error) {
	return s.store.List(ctx)
}

// Get resolves a product by its integer id (the spec types the path
// parameter as int64). Returns 400 for non-integers, 404 for unknown ids.
func (s *Products) Get(ctx context.Context, rawID string) (*domain.Product, error) {
	id, ok := canonicalID(rawID)
	if !ok {
		return nil, domain.ErrBadRequest("Invalid ID supplied")
	}
	p, err := s.store.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, domain.ErrNotFound("Product not found")
	}
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Create validates and inserts a new catalog product (extension endpoint).
func (s *Products) Create(ctx context.Context, np domain.NewProduct) (*domain.Product, error) {
	var problems []string
	np.Name = strings.TrimSpace(np.Name)
	np.Category = strings.TrimSpace(np.Category)
	if np.Name == "" || len(np.Name) > MaxNameLen {
		problems = append(problems, fmt.Sprintf("name is required (1..%d chars)", MaxNameLen))
	}
	if np.Category == "" || len(np.Category) > MaxCategory {
		problems = append(problems, fmt.Sprintf("category is required (1..%d chars)", MaxCategory))
	}
	if np.Price <= 0 || math.IsNaN(np.Price) || math.IsInf(np.Price, 0) || np.Price > MaxPrice {
		problems = append(problems, fmt.Sprintf("price must be > 0 and <= %d", MaxPrice))
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, domain.ErrValidation(strings.Join(problems, "; "))
	}
	return s.store.Create(ctx, np)
}

// canonicalID validates a strictly-decimal id and returns its canonical form.
// Rejects signs, spaces, hex, floats; accepts leading zeros but canonicalizes
// them ("007" → "7") so lookups behave consistently across stores.
func canonicalID(raw string) (string, bool) {
	if raw == "" || len(raw) > 18 {
		return "", false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return "", false
		}
	}
	trimmed := strings.TrimLeft(raw, "0")
	if trimmed == "" {
		trimmed = "0"
	}
	return trimmed, true
}

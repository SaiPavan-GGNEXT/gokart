// Package domain holds the core business types shared by every layer.
// It has no dependencies on transport, storage, or framework code.
package domain

import "fmt"

// Product is a purchasable catalog item.
// IDs are numeric but serialized as strings, matching the OpenAPI examples ("10").
type Product struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	Category string  `json:"category"`
}

// NewProduct carries the fields a client may supply when creating a product.
// The ID is always assigned by the store, never by the caller.
type NewProduct struct {
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	Category string  `json:"category"`
}

// OrderItem is one line of an order request/response.
type OrderItem struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

// Order is a placed order, shaped exactly as the OpenAPI Order schema:
// id, the requested items, and the expanded product details.
type Order struct {
	ID       string      `json:"id"`
	Items    []OrderItem `json:"items"`
	Products []Product   `json:"products"`

	// CouponCode is persisted for auditing but intentionally not serialized:
	// the spec's Order schema does not define it.
	CouponCode string `json:"-"`
}

// APIResponse is the uniform error/status envelope from the OpenAPI spec.
type APIResponse struct {
	Code    int    `json:"code"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

// Error is a transport-agnostic business error carrying the HTTP status the
// API layer should map it to. Services return *Error for expected failures;
// anything else is treated as an internal error.
type Error struct {
	Status  int    // HTTP status code
	Type    string // machine-readable kind, e.g. "validation_error"
	Message string // human-readable detail
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Type, e.Message) }

// Well-known error constructors keep status mapping in one place.

func ErrBadRequest(msg string) *Error {
	return &Error{Status: 400, Type: "invalid_input", Message: msg}
}

func ErrUnauthorized(msg string) *Error {
	return &Error{Status: 401, Type: "unauthorized", Message: msg}
}

func ErrForbidden(msg string) *Error {
	return &Error{Status: 403, Type: "forbidden", Message: msg}
}

func ErrNotFound(msg string) *Error {
	return &Error{Status: 404, Type: "not_found", Message: msg}
}

func ErrValidation(msg string) *Error {
	return &Error{Status: 422, Type: "validation_error", Message: msg}
}

func ErrUnavailable(msg string) *Error {
	return &Error{Status: 503, Type: "service_unavailable", Message: msg}
}

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/domain"
	"github.com/SAIPAVANKUMARGUNDA/go-kart/internal/service"
)

type handlers struct {
	products *service.Products
	orders   *service.Orders
	ready    func(ctx context.Context) (map[string]any, error)
}

// --- products ---------------------------------------------------------------

func (h *handlers) listProducts(c *fiber.Ctx) error {
	items, err := h.products.List(c.Context())
	if err != nil {
		return failDomain(c, err)
	}
	return c.JSON(items) // always an array, [] when empty — never null
}

func (h *handlers) getProduct(c *fiber.Ctx) error {
	p, err := h.products.Get(c.Context(), c.Params("productId"))
	if err != nil {
		return failDomain(c, err)
	}
	return c.JSON(p)
}

// productPayload uses pointers so "field missing" (nil) is distinguishable
// from zero values, and typed fields so wrong JSON types fail decoding.
type productPayload struct {
	Name     *string  `json:"name"`
	Price    *float64 `json:"price"`
	Category *string  `json:"category"`
}

func (h *handlers) createProduct(c *fiber.Ctx) error {
	var body productPayload
	if err := decodeJSON(c.Body(), &body); err != nil {
		return fail(c, fiber.StatusBadRequest, "invalid_input", err.Error())
	}
	if body.Name == nil || body.Price == nil || body.Category == nil {
		return fail(c, fiber.StatusBadRequest, "invalid_input",
			"name, price and category are required")
	}
	p, err := h.products.Create(c.Context(), domain.NewProduct{
		Name: *body.Name, Price: *body.Price, Category: *body.Category,
	})
	if err != nil {
		return failDomain(c, err)
	}
	return c.Status(fiber.StatusCreated).JSON(p)
}

// --- orders -----------------------------------------------------------------

type orderItemPayload struct {
	ProductID *string `json:"productId"`
	Quantity  *int    `json:"quantity"`
}

type orderPayload struct {
	CouponCode *string             `json:"couponCode"`
	Items      *[]orderItemPayload `json:"items"`
}

func (h *handlers) placeOrder(c *fiber.Ctx) error {
	var body orderPayload
	if err := decodeJSON(c.Body(), &body); err != nil {
		return fail(c, fiber.StatusBadRequest, "invalid_input", err.Error())
	}
	if body.Items == nil || len(*body.Items) == 0 {
		return fail(c, fiber.StatusBadRequest, "invalid_input",
			"items is required and must not be empty")
	}

	req := service.OrderRequest{CouponCode: body.CouponCode}
	for i, it := range *body.Items {
		if it.ProductID == nil {
			return fail(c, fiber.StatusBadRequest, "invalid_input",
				fmt.Sprintf("items[%d].productId is required", i))
		}
		if it.Quantity == nil {
			return fail(c, fiber.StatusBadRequest, "invalid_input",
				fmt.Sprintf("items[%d].quantity is required", i))
		}
		req.Items = append(req.Items, domain.OrderItem{
			ProductID: *it.ProductID, Quantity: *it.Quantity,
		})
	}

	order, err := h.orders.Place(c.Context(), req)
	if err != nil {
		return failDomain(c, err)
	}
	return c.JSON(order)
}

// --- health -----------------------------------------------------------------

// healthz is liveness: the process is up and serving.
func (h *handlers) healthz(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok", "time": time.Now().UTC()})
}

// readyz is readiness: every dependency answers, and the payload reports
// exactly which coupon truth this instance serves (index provenance).
func (h *handlers) readyz(c *fiber.Ctx) error {
	info, err := h.ready(c.Context())
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).
			JSON(fiber.Map{"status": "unavailable", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "ready", "checks": info})
}

// decodeJSON decodes strictly: type mismatches, syntax errors, and trailing
// garbage all fail with a caller-friendly message. Unknown fields are
// tolerated (forward compatibility) — documented in the README.
func decodeJSON(body []byte, dst any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return errors.New("request body is required")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(dst); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return fmt.Errorf("field %q must be of type %s", typeErr.Field, typeErr.Type)
		}
		return errors.New("request body is not valid JSON")
	}
	if dec.More() {
		return errors.New("request body contains trailing data")
	}
	// Guard against a second JSON document separated by whitespace.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body contains trailing data")
	}
	return nil
}

package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/SaiPavan-GGNEXT/gokart/internal/config"
	"github.com/SaiPavan-GGNEXT/gokart/internal/coupon"
	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
	"github.com/SaiPavan-GGNEXT/gokart/internal/service"
	"github.com/SaiPavan-GGNEXT/gokart/internal/store/memory"
)

// newTestApp wires a full app: memory stores, a REAL index validator over a
// tiny index file, seeded products — the same wiring as production.
func newTestApp(t *testing.T) *fiber.App {
	t.Helper()

	idxPath := filepath.Join(t.TempDir(), "coupons.idx")
	if err := coupon.WriteIndex(idxPath,
		coupon.IndexMeta{BuiltAt: time.Now().UTC(), ToolVersion: "test"},
		[]string{"HAPPYHRS", "FIFTYOFF", "OVER9000"}); err != nil {
		t.Fatal(err)
	}
	validator, err := coupon.NewIndexValidator(idxPath)
	if err != nil {
		t.Fatal(err)
	}

	products := memory.NewProductStore()
	orders := memory.NewOrderStore()
	for _, p := range []struct {
		id string
		np domain.NewProduct
	}{
		{"10", domain.NewProduct{Name: "Chicken Waffle", Price: 12.99, Category: "Waffle"}},
		{"2", domain.NewProduct{Name: "Latte", Price: 4.5, Category: "Drinks"}},
	} {
		if _, err := products.CreateWithID(context.Background(), p.id, p.np); err != nil {
			t.Fatal(err)
		}
	}

	cfg := &config.Config{
		Port:            "0",
		APIKeys:         map[string][]string{"apitest": {"create_order", "manage_products"}, "noscope": {}},
		BodyLimitBytes:  64 * 1024,
		RateLimitRPM:    0, // deterministic tests
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		IdleTimeout:     5 * time.Second,
		ShutdownTimeout: time.Second,
		Store:           "memory",
		Validator:       "index",
	}
	return New(cfg, Deps{
		Products: service.NewProducts(products),
		Orders:   service.NewOrders(products, orders, validator),
		Ready: func(context.Context) (map[string]any, error) {
			return map[string]any{"validator": validator.Info()}, nil
		},
	})
}

type call struct {
	method, path, body, apiKey string
}

func do(t *testing.T, app *fiber.App, c call) (int, string) {
	t.Helper()
	var body io.Reader
	if c.body != "" {
		body = strings.NewReader(c.body)
	}
	req := httptest.NewRequest(c.method, c.path, body)
	if c.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("api_key", c.apiKey)
	}
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("%s %s: %v", c.method, c.path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestEdgeCaseMatrix is the README's edge-case table, executable.
func TestEdgeCaseMatrix(t *testing.T) {
	app := newTestApp(t)
	okOrder := `{"items":[{"productId":"10","quantity":2}]}`

	cases := []struct {
		name       string
		call       call
		wantStatus int
		wantSubstr string
	}{
		// products
		{"list products", call{"GET", "/api/product", "", ""}, 200, `"Chicken Waffle"`},
		{"get product", call{"GET", "/api/product/10", "", ""}, 200, `"id":"10"`},
		{"non-integer id", call{"GET", "/api/product/abc", "", ""}, 400, "Invalid ID"},
		{"float id", call{"GET", "/api/product/1.5", "", ""}, 400, "Invalid ID"},
		{"signed id", call{"GET", "/api/product/+10", "", ""}, 400, "Invalid ID"},
		{"overflow id", call{"GET", "/api/product/99999999999999999999", "", ""}, 400, "Invalid ID"},
		{"unknown id", call{"GET", "/api/product/999", "", ""}, 404, "Product not found"},
		{"products are public", call{"GET", "/api/product", "", ""}, 200, ""},

		// auth
		{"order without key", call{"POST", "/api/order", okOrder, ""}, 401, "api_key header is required"},
		{"order with bad key", call{"POST", "/api/order", okOrder, "nope"}, 401, "invalid api_key"},
		{"order with scopeless key", call{"POST", "/api/order", okOrder, "noscope"}, 403, "create_order"},

		// order structure
		{"happy order", call{"POST", "/api/order", okOrder, "apitest"}, 200, `"products"`},
		{"empty body", call{"POST", "/api/order", "", "apitest"}, 400, "body is required"},
		{"bad json", call{"POST", "/api/order", `{nope`, "apitest"}, 400, "not valid JSON"},
		{"missing items", call{"POST", "/api/order", `{}`, "apitest"}, 400, "items is required"},
		{"empty items", call{"POST", "/api/order", `{"items":[]}`, "apitest"}, 400, "items is required"},
		{"quantity as string", call{"POST", "/api/order", `{"items":[{"productId":"10","quantity":"2"}]}`, "apitest"}, 400, "must be of type int"},
		{"productId as number", call{"POST", "/api/order", `{"items":[{"productId":10,"quantity":2}]}`, "apitest"}, 400, "must be of type string"},
		{"missing quantity", call{"POST", "/api/order", `{"items":[{"productId":"10"}]}`, "apitest"}, 400, "quantity is required"},
		{"trailing garbage", call{"POST", "/api/order", okOrder + `{"x":1}`, "apitest"}, 400, "trailing"},
		{"unknown fields tolerated", call{"POST", "/api/order", `{"items":[{"productId":"10","quantity":1}],"note":"hi"}`, "apitest"}, 200, `"id"`},

		// order semantics
		{"zero quantity", call{"POST", "/api/order", `{"items":[{"productId":"10","quantity":0}]}`, "apitest"}, 422, "quantity must be at least 1"},
		{"negative quantity", call{"POST", "/api/order", `{"items":[{"productId":"10","quantity":-3}]}`, "apitest"}, 422, "quantity must be at least 1"},
		{"unknown product", call{"POST", "/api/order", `{"items":[{"productId":"777","quantity":1}]}`, "apitest"}, 422, "unknown productId"},
		{"multiple problems reported", call{"POST", "/api/order", `{"items":[{"productId":"10","quantity":0},{"productId":"","quantity":1}]}`, "apitest"}, 422, ";"},

		// coupons (validated against a real index file)
		{"valid coupon", call{"POST", "/api/order", `{"couponCode":"HAPPYHRS","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 200, `"id"`},
		{"whitespace coupon accepted", call{"POST", "/api/order", `{"couponCode":"  FIFTYOFF  ","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 200, `"id"`},
		{"invalid coupon", call{"POST", "/api/order", `{"couponCode":"SUPER100","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 422, "invalid promo code"},
		{"short coupon", call{"POST", "/api/order", `{"couponCode":"ABC","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 422, "invalid promo code"},
		{"long coupon", call{"POST", "/api/order", `{"couponCode":"ABCDEFGHIJK","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 422, "invalid promo code"},
		{"empty coupon string", call{"POST", "/api/order", `{"couponCode":"","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 422, "invalid promo code"},
		{"lowercase coupon", call{"POST", "/api/order", `{"couponCode":"happyhrs","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 422, "invalid promo code"},
		{"padding trap", call{"POST", "/api/order", `{"couponCode":"OVER900000","items":[{"productId":"10","quantity":1}]}`, "apitest"}, 422, "invalid promo code"},
		{"coupon null means absent", call{"POST", "/api/order", `{"couponCode":null,"items":[{"productId":"10","quantity":1}]}`, "apitest"}, 200, `"id"`},

		// product creation (extension endpoint)
		{"create product", call{"POST", "/api/product", `{"name":"Mocha","price":5.1,"category":"Drinks"}`, "apitest"}, 201, `"Mocha"`},
		{"create product no key", call{"POST", "/api/product", `{"name":"X","price":1,"category":"Y"}`, ""}, 401, ""},
		{"create product wrong scope", call{"POST", "/api/product", `{"name":"X","price":1,"category":"Y"}`, "noscope"}, 403, "manage_products"},
		{"create product negative price", call{"POST", "/api/product", `{"name":"X","price":-2,"category":"Y"}`, "apitest"}, 422, "price"},
		{"create product missing fields", call{"POST", "/api/product", `{"name":"X"}`, "apitest"}, 400, "required"},
		{"create product NaN-proof", call{"POST", "/api/product", `{"name":"X","price":"free","category":"Y"}`, "apitest"}, 400, "must be of type"},

		// routing & protocol
		{"unknown route", call{"GET", "/api/nothing", "", ""}, 404, "not_found"},
		{"root route", call{"GET", "/", "", ""}, 404, "not_found"},
		{"wrong method on products", call{"DELETE", "/api/product", "", ""}, 405, "method_not_allowed"},
		{"wrong method on order", call{"GET", "/api/order", "", ""}, 405, "method_not_allowed"},
		{"healthz", call{"GET", "/healthz", "", ""}, 200, "ok"},
		{"readyz", call{"GET", "/readyz", "", ""}, 200, "ready"},
		{"openapi served", call{"GET", "/openapi.yaml", "", ""}, 200, "openapi: 3.1.0"},
		{"swagger ui served", call{"GET", "/docs", "", ""}, 200, "swagger-ui"},
		{"extended spec served", call{"GET", "/docs/openapi.yaml", "", ""}, 200, "implementation docs"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := do(t, app, tc.call)
			if status != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", status, tc.wantStatus, body)
			}
			if tc.wantSubstr != "" && !strings.Contains(body, tc.wantSubstr) {
				t.Fatalf("body %q does not contain %q", body, tc.wantSubstr)
			}
		})
	}
}

// TestCORSPreflight: a browser frontend on another origin must be able to
// preflight POST /api/order with the api_key header.
func TestCORSPreflight(t *testing.T) {
	app := newTestApp(t)
	req := httptest.NewRequest("OPTIONS", "/api/order", nil)
	req.Header.Set("Origin", "https://frontend.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "api_key, content-type")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode != 200 {
		t.Fatalf("preflight status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("missing Access-Control-Allow-Origin on preflight response")
	}
}

// TestErrorEnvelopeShape: every error, including router-level ones, must be
// the spec's ApiResponse {code, type, message}.
func TestErrorEnvelopeShape(t *testing.T) {
	app := newTestApp(t)
	for _, c := range []call{
		{"GET", "/api/nothing", "", ""},
		{"DELETE", "/api/product", "", ""},
		{"GET", "/api/product/abc", "", ""},
		{"POST", "/api/order", `{}`, "apitest"},
		{"POST", "/api/order", `{"items":[{"productId":"1","quantity":1}]}`, ""},
	} {
		status, body := do(t, app, c)
		var env domain.APIResponse
		if err := json.Unmarshal([]byte(body), &env); err != nil {
			t.Fatalf("%s %s: body is not an APIResponse: %s", c.method, c.path, body)
		}
		if env.Code != status || env.Type == "" || env.Message == "" {
			t.Fatalf("%s %s: bad envelope %+v (status %d)", c.method, c.path, env, status)
		}
	}
}

// TestOrderResponseIsSpecShaped: exactly id/items/products, nothing else —
// the spec's Order schema defines no other fields.
func TestOrderResponseIsSpecShaped(t *testing.T) {
	app := newTestApp(t)
	status, body := do(t, app, call{"POST", "/api/order",
		`{"couponCode":"HAPPYHRS","items":[{"productId":"10","quantity":1},{"productId":"10","quantity":2}]}`, "apitest"})
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		if k != "id" && k != "items" && k != "products" {
			t.Errorf("unexpected field %q in Order response", k)
		}
	}
	prods := m["products"].([]any)
	if len(prods) != 1 {
		t.Errorf("products must be unique per product: got %d entries", len(prods))
	}
	items := m["items"].([]any)
	if len(items) != 2 {
		t.Errorf("items must be echoed as sent: got %d", len(items))
	}
}

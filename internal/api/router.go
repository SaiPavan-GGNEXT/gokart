// Package api wires the Fiber application: routes, middleware, error
// handling, and the uniform response envelope.
package api

import (
	"context"
	_ "embed"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"

	"github.com/SaiPavan-GGNEXT/gokart/internal/config"
	"github.com/SaiPavan-GGNEXT/gokart/internal/service"
)

//go:embed openapi.yaml
var openapiYAML []byte

// The pinned challenge spec stays untouched at /openapi.yaml; /docs serves an
// interactive Swagger UI backed by an extended spec (relative server URL, so
// "Try it out" targets THIS deployment, plus the documented extensions).
//
//go:embed openapi-extended.yaml
var openapiExtendedYAML []byte

//go:embed docs.html
var docsHTML []byte

// Deps are the wired dependencies the router needs.
type Deps struct {
	Products *service.Products
	Orders   *service.Orders
	// Ready aggregates dependency health for /readyz.
	Ready func(ctx context.Context) (map[string]any, error)
}

// New builds the Fiber app. It does not listen; cmd/server owns the lifecycle.
func New(cfg *config.Config, deps Deps) *fiber.App {
	app := fiber.New(fiber.Config{
		AppName:               "kart-challenge",
		DisableStartupMessage: true,
		ReadTimeout:           cfg.ReadTimeout,
		WriteTimeout:          cfg.WriteTimeout,
		IdleTimeout:           cfg.IdleTimeout,
		BodyLimit:             cfg.BodyLimitBytes,
		// Every error Fiber itself raises (404, 405, 413, panics converted by
		// recover) flows through here, so even framework errors keep the
		// spec's APIResponse envelope.
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code, typ, msg := fiber.StatusInternalServerError, "internal_error", "internal server error"
			if fe, ok := err.(*fiber.Error); ok {
				code, msg = fe.Code, fe.Message
				switch code {
				case fiber.StatusNotFound:
					typ, msg = "not_found", "resource not found"
				case fiber.StatusMethodNotAllowed:
					typ, msg = "method_not_allowed", "method not allowed for this resource"
				case fiber.StatusRequestEntityTooLarge:
					typ, msg = "payload_too_large", "request body exceeds the size limit"
				default:
					typ = "error"
				}
			}
			return fail(c, code, typ, msg)
		},
	})

	h := &handlers{products: deps.Products, orders: deps.Orders, ready: deps.Ready}

	app.Use(requestid.New(requestid.Config{ContextKey: "requestid"}))
	app.Use(recover.New()) // panics become 500s; the process never dies mid-request
	app.Use(requestLogger())
	// Browser clients (the separate kart-UI frontend) live on another origin;
	// api_key is a plain header (no cookies), so wildcard origins are safe.
	// Lock down via CORS_ORIGINS in environments that warrant it.
	app.Use(cors.New(cors.Config{
		AllowOrigins: cfg.CORSOrigins,
		AllowMethods: "GET,POST,HEAD,OPTIONS",
		AllowHeaders: "Origin, Content-Type, Accept, api_key",
		MaxAge:       3600,
	}))

	// Operational endpoints (outside /api, unauthenticated by design).
	app.Get("/healthz", h.healthz)
	app.Get("/readyz", h.readyz)
	app.Get("/openapi.yaml", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, "application/yaml")
		return c.Send(openapiYAML)
	})
	app.Get("/docs", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.Send(docsHTML)
	})
	app.Get("/docs/openapi.yaml", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, "application/yaml")
		return c.Send(openapiExtendedYAML)
	})

	api := app.Group("/api")
	if cfg.RateLimitRPM > 0 {
		api.Use(limiter.New(limiter.Config{
			Max:        cfg.RateLimitRPM,
			Expiration: 60_000_000_000, // 1 minute in ns
			LimitReached: func(c *fiber.Ctx) error {
				return fail(c, fiber.StatusTooManyRequests, "rate_limited",
					"too many requests, slow down")
			},
		}))
	}

	// Spec endpoints. Product reads are public — the spec declares security
	// only on POST /order (a deliberate difference from implementations that
	// blanket-authenticate everything).
	api.Get("/product", h.listProducts)
	api.Get("/product/:productId", h.getProduct)
	api.Post("/order", requireScope(cfg.APIKeys, config.ScopeCreateOrder), h.placeOrder)

	// Extension endpoint (documented in README as beyond-spec): catalog
	// insertion, guarded by its own scope.
	api.Post("/product", requireScope(cfg.APIKeys, config.ScopeManageProducts), h.createProduct)

	// Anything unmatched gets the JSON envelope, not a plain-text 404 — and
	// a known path hit with the wrong method gets a proper 405 + Allow.
	registered := app.GetRoutes(true)
	app.Use(func(c *fiber.Ctx) error {
		allowed := allowedMethods(registered, c.Path())
		if len(allowed) > 0 {
			c.Set(fiber.HeaderAllow, strings.Join(allowed, ", "))
			return fiber.ErrMethodNotAllowed
		}
		return fiber.ErrNotFound
	})

	return app
}

// allowedMethods returns the methods registered for a concrete path, by
// matching it against the route table (":param" segments match anything).
func allowedMethods(routes []fiber.Route, path string) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range routes {
		if r.Method == fiber.MethodHead || !patternMatches(r.Path, path) || seen[r.Method] {
			continue
		}
		seen[r.Method] = true
		out = append(out, r.Method)
	}
	sort.Strings(out)
	return out
}

func patternMatches(pattern, path string) bool {
	if pattern == "/" { // middleware mounts; never counts as a resource
		return false
	}
	ps := strings.Split(strings.TrimSuffix(pattern, "/"), "/")
	xs := strings.Split(strings.TrimSuffix(path, "/"), "/")
	if len(ps) != len(xs) {
		return false
	}
	for i := range ps {
		if strings.HasPrefix(ps[i], ":") {
			if xs[i] == "" {
				return false
			}
			continue
		}
		if ps[i] != xs[i] {
			return false
		}
	}
	return true
}

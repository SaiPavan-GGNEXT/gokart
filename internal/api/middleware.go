package api

import (
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/SaiPavan-GGNEXT/gokart/internal/config"
)

// requireScope authenticates the api_key header and authorizes the scope.
//
// Semantics (documented in README):
//   - header absent or key unknown → 401 (we cannot say who you are)
//   - key known but scope missing  → 403 (we know you; you may not do this)
//
// The spec declares `api_key: [create_order]` scoped security on POST /order;
// modeling keys→scopes makes 403 genuinely reachable instead of decorative.
// The api_key value itself is never logged.
func requireScope(keys map[string][]string, scope string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		key := c.Get("api_key")
		if key == "" {
			return fail(c, fiber.StatusUnauthorized, "unauthorized", "api_key header is required")
		}
		scopes, known := keys[key]
		if !known {
			return fail(c, fiber.StatusUnauthorized, "unauthorized", "invalid api_key")
		}
		for _, s := range scopes {
			if s == scope {
				return c.Next()
			}
		}
		return fail(c, fiber.StatusForbidden, "forbidden",
			"api_key does not have the '"+scope+"' scope")
	}
}

// requestLogger emits one structured line per request.
func requestLogger() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		slog.Info("http",
			"request_id", requestID(c),
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"duration_ms", float64(time.Since(start).Microseconds())/1000.0,
			"ip", c.IP(),
		)
		return err
	}
}

// scopesForDocs is referenced by the README generator/tests to keep docs and
// code agreeing on which scope guards which route.
var scopesForDocs = map[string]string{
	"POST /api/order":   config.ScopeCreateOrder,
	"POST /api/product": config.ScopeManageProducts,
}

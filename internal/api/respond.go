package api

import (
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v2"

	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
)

// fail writes the uniform APIResponse error envelope.
func fail(c *fiber.Ctx, status int, typ, msg string) error {
	return c.Status(status).JSON(domain.APIResponse{Code: status, Type: typ, Message: msg})
}

// failDomain maps any error returned by a service onto the envelope:
// *domain.Error keeps its status; everything else is a logged 500.
func failDomain(c *fiber.Ctx, err error) error {
	var de *domain.Error
	if errors.As(err, &de) {
		return fail(c, de.Status, de.Type, de.Message)
	}
	slog.Error("internal error",
		"request_id", requestID(c), "path", c.Path(), "error", err)
	return fail(c, fiber.StatusInternalServerError, "internal_error",
		"something went wrong, please retry")
}

func requestID(c *fiber.Ctx) string {
	if id, ok := c.Locals("requestid").(string); ok {
		return id
	}
	return ""
}

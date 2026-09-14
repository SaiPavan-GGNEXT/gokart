// Package seed provides the embedded default catalog. The assignment supplies
// no product data anywhere (spec, repo, or demo — which is offline), so this
// catalog is our own, keeping the spec's documented example: id 10 = Chicken
// Waffle, category Waffle.
//
// Seeding runs at startup only when the store is empty (SEED_PRODUCTS=true,
// the default). The same catalog can instead be inserted through the public
// POST /api/product endpoint — see scripts/seed_products.sh.
package seed

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/saipavankumar/kart-challenge/internal/domain"
)

//go:embed products.json
var productsJSON []byte

// SeedProduct is one catalog row with its stable id.
type SeedProduct struct {
	ID string `json:"id"`
	domain.NewProduct
}

// Products returns the embedded catalog.
func Products() ([]SeedProduct, error) {
	var out []SeedProduct
	if err := json.Unmarshal(productsJSON, &out); err != nil {
		return nil, fmt.Errorf("seed catalog is corrupt: %w", err)
	}
	return out, nil
}

// RawJSON exposes the catalog bytes (used by scripts and tests).
func RawJSON() []byte { return productsJSON }

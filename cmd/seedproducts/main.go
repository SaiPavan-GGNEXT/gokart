// Command seedproducts inserts the default catalog THROUGH the public API
// (POST /api/product), exactly as an external consumer would — demonstrating
// the catalog-management extension endpoint end to end.
//
//	go run ./cmd/seedproducts                                  # local server
//	go run ./cmd/seedproducts -url https://gokart-zba3.onrender.com
//
// Idempotence note: the endpoint assigns fresh ids, so run this against an
// empty catalog (start the server with SEED_PRODUCTS=false) or expect
// name-level duplicates.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/SaiPavan-GGNEXT/gokart/internal/domain"
	"github.com/SaiPavan-GGNEXT/gokart/internal/seed"
)

func main() {
	apiURL := flag.String("url", envOr("API_URL", "http://localhost:8080"), "API base URL")
	apiKey := flag.String("key", envOr("API_KEY", "apitest"), "api_key holding the manage_products scope")
	flag.Parse()

	rows, err := seed.Products()
	if err != nil {
		fmt.Fprintln(os.Stderr, "seedproducts:", err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for _, r := range rows {
		created, err := createProduct(client, *apiURL, *apiKey, r.NewProduct)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAILED %s: %v\n", r.Name, err)
			os.Exit(1)
		}
		fmt.Printf("created id=%-3s %s\n", created.ID, created.Name)
	}
	fmt.Printf("done: %d/%d products created via the API\n", len(rows), len(rows))
}

func createProduct(client *http.Client, apiURL, apiKey string, np domain.NewProduct) (*domain.Product, error) {
	body, err := json.Marshal(np)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, apiURL+"/api/product", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api_key", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, respBody)
	}
	var p domain.Product
	if err := json.Unmarshal(respBody, &p); err != nil {
		return nil, fmt.Errorf("bad response body: %w", err)
	}
	return &p, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

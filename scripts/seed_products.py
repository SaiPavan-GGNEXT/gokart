#!/usr/bin/env python3
"""Seed the product catalog THROUGH the public API (POST /api/product).

This demonstrates catalog management as an API consumer would do it, using
only the standard library. Idempotence note: the endpoint assigns fresh ids,
so run this against an empty catalog (SEED_PRODUCTS=false) or expect
duplicates by name.

  API_URL=http://localhost:8080 API_KEY=apitest python3 scripts/seed_products.py
"""
import json
import os
import sys
import urllib.request
import urllib.error

API_URL = os.environ.get("API_URL", "http://localhost:8080").rstrip("/")
API_KEY = os.environ.get("API_KEY", "apitest")
CATALOG = os.path.join(os.path.dirname(__file__), "..", "internal", "seed", "products.json")


def main() -> int:
    with open(CATALOG, encoding="utf-8") as f:
        products = json.load(f)

    created = 0
    for p in products:
        body = json.dumps(
            {"name": p["name"], "price": p["price"], "category": p["category"]}
        ).encode()
        req = urllib.request.Request(
            f"{API_URL}/api/product",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "api_key": API_KEY},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                out = json.loads(resp.read())
                print(f"created id={out['id']:>3}  {out['name']}")
                created += 1
        except urllib.error.HTTPError as e:
            print(f"FAILED {p['name']}: HTTP {e.code} {e.read().decode()}", file=sys.stderr)
            return 1
    print(f"done: {created}/{len(products)} products created via the API")
    return 0


if __name__ == "__main__":
    sys.exit(main())

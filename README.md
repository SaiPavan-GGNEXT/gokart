# kart-challenge — Order Food Online API

A Go implementation of the [Order Food Online OpenAPI spec](api/openapi.yaml)
(advanced backend challenge), with promo-code validation against the full
313-million-line coupon corpus.

**The core idea:** a coupon's validity depends only on the corpus, never on
the request — so all heavy work runs **once, offline**, in an indexer that
reduces 2.1 GB of gzipped codes to a **736-byte exact index** of the codes
present in ≥2 files. The API validates coupons with an **18 ns in-memory
lookup** (benchmarked, 0 allocations). Full reasoning, measurements, and
trade-offs: [docs/DESIGN.md](docs/DESIGN.md).

```
BUILD TIME (once per corpus)                    REQUEST TIME (every order)
couponbase{1,2,3}.gz ─► cmd/indexer ─► coupons.idx ─► loaded at startup ─► binary search
  3.1 GB, 313M lines       ~16 s          736 B          verified, once      18 ns, exact
```

## Quick start (no downloads, real validation)

The repo ships the index **built from the full real corpus** (provenance:
[data/MANIFEST.json](data/MANIFEST.json) — source sha256s are also embedded
in the index header and exposed at `/readyz`). So:

```bash
docker compose up --build     # or: make run   (Go 1.26+)
```

```bash
curl localhost:8080/api/product
curl -X POST localhost:8080/api/order \
  -H 'api_key: apitest' -H 'content-type: application/json' \
  -d '{"couponCode":"HAPPYHRS","items":[{"productId":"10","quantity":2}]}'
# → 200 with the order.  Try "SUPER100" → 422 invalid promo code.
```

Verify the index yourself anytime: `make corpus && make index` (downloads
2.1 GB, rebuilds in ~16 s, prints per-file stats + sha256s).

Variants (extensibility seams, live):

```bash
docker compose --profile postgres up   # durable order store (port 8082)
docker compose --profile redis up      # Redis-backed validator (port 8081)
```

## API

Base path `/api` (per the spec's server URL). Three ways to explore:
**interactive Swagger UI at [`/docs`](https://gokart-zba3.onrender.com/docs)**
("Try it out" targets the serving host; Authorize with `apitest`),
[postman_collection.json](postman_collection.json), or the pinned original
spec served at `/openapi.yaml`.

| Endpoint | Auth | Success | Errors |
|---|---|---|---|
| `GET /api/product` | none | 200 `[Product]` | — |
| `GET /api/product/{productId}` | none | 200 `Product` | 400 non-integer id, 404 unknown |
| `POST /api/order` | `api_key` + `create_order` scope | 200 `Order` | 400 / 401 / 403 / 422 (below) |
| `POST /api/product` † | `api_key` + `manage_products` scope | 201 `Product` | 400 / 401 / 403 / 422 |
| `GET /healthz`, `GET /readyz` | none | liveness / readiness + index provenance | 503 when not ready |

† **Extension beyond the spec** (the assignment invites additional APIs):
catalog insertion, used for seeding — `make seed` inserts the default catalog
*through this endpoint* (`cmd/seedproducts`, a small Go client). At startup
the server also auto-seeds an empty catalog (`SEED_PRODUCTS=true` default),
since the assignment ships no product data and the demo server is offline.

**API keys** (config `API_KEYS`, JSON): default grants `apitest` both scopes
(the spec's documented key), plus scope-less `apitest_noscope` so 403 is
demonstrable. Keys are never logged.

### Error semantics (the spec lists codes; meanings are our documented choice)

| Code | Meaning | Examples |
|---|---|---|
| 400 | structurally invalid | malformed JSON, wrong types (`quantity: "2"`), missing `items`, trailing garbage, non-integer path id |
| 401 | cannot identify caller | `api_key` missing or unknown |
| 403 | identified, not permitted | key lacks the route's scope |
| 404 | resource absent | unknown product id; unknown route |
| 405 | wrong method (`Allow` header set) | `DELETE /api/product` |
| 413 | body over 1 MiB | oversized payload |
| 422 | semantically invalid | unknown `productId`, `quantity < 1`, **invalid promo code** |
| 429 | rate limited (per IP) | default 300 req/min |
| 503 | dependency cannot answer | remote validator down (coupons **fail closed** — never guessed) |

Every error, including router-level 404/405, uses the spec's
`ApiResponse {code, type, message}` envelope. The `Order` response is
strictly spec-shaped (`id`, `items`, `products` — no invented fields; the
spec defines no discount semantics, so validation gates order placement only).

### Coupon rules implemented

Valid iff **8–10 bytes of `A-Z0-9`** (after trimming) **and present in ≥2 of
the 3 corpus files** — where a code appearing twice in *one* file counts
once. Verified against the full corpus: `HAPPYHRS` (files 1+3) and `FIFTYOFF`
(all 3) are valid, `SUPER100` (file 1 only) is not — all three match the
assignment's documented examples. Case-sensitive by decision (the corpus is
uppercase-only). One shared `normalize()` is used by both the indexer and the
request path, so they cannot disagree.

## Edge-case matrix (each row is an automated test — `internal/api/api_test.go`)

| Input | Response |
|---|---|
| `GET /api/product/abc`, `/1.5`, `/+10`, 20-digit overflow | 400 |
| `GET /api/product/999` | 404 |
| order: no `api_key` / unknown key / scope-less key | 401 / 401 / 403 |
| order: empty body, `{}`, `items: []`, malformed JSON, trailing garbage | 400 |
| order: `quantity: "2"` (string), `productId: 10` (number) | 400 (type-strict) |
| order: `quantity: 0` or negative, unknown `productId` | 422 (all problems reported at once) |
| coupon: 7 or 11 chars, lowercase, empty string, unicode | 422 (shape gate, no lookup) |
| coupon: `SUPER100`, `MOODYHRS` (each in exactly 1 file) | 422 |
| coupon: `OVER900000` (padding trap vs valid `OVER9000`) | 422 |
| coupon: `"  FIFTYOFF  "` (whitespace) | 200 (trimmed, valid) |
| `DELETE /api/product` | 405 + `Allow: GET, POST` |
| unknown route | 404, JSON envelope |
| 50 MB body | 413 (1 MiB limit) |
| handler panic / slow clients | 500 + recovery / read-write-idle timeouts |
| concurrent orders | race-free (`go test -race` in CI) |

## Robustness & operations

- **Fail-fast startup** — missing/corrupt index (magic, size, CRC verified) ⇒
  exit with an actionable message; never serve coupon answers you can't trust.
  The dev-only static validator requires an explicit `VALIDATOR=static` +
  `ENV=dev` so it can never silently mask a missing index.
- **Graceful shutdown** — SIGTERM drains in-flight requests (10 s budget).
- **Observability** — structured JSON logs with request ids; `/readyz`
  reports the live validator's provenance (index build time, source sha256s,
  code count): "which coupon truth is this instance serving?" is one curl away.
- **Indexer integrity** — any gzip/CRC/read error aborts the build with no
  output file (all valid codes sit in the last lines of the corpus files, so
  tolerating truncation would silently break everything — tested).

## Configuration (env)

| Var | Default | Notes |
|---|---|---|
| `PORT` | `8080` | Render injects this |
| `STORE` | `memory` | `postgres` requires `DATABASE_URL` (pgx pool, schema auto-applied, transactional orders) |
| `VALIDATOR` | `index` | `redis` (`REDIS_ADDR`, seed via `cmd/seedredis`); `static` is a dev-only fixture |
| `COUPON_INDEX` | `data/coupons.idx` | local path **or `https://` URL** (e.g. an S3 object); fail-fast if unreadable |
| `COUPON_RELOAD_INTERVAL` | `0` (off) | with a URL index: poll (ETag) and **hot-swap on change — live coupon updates, no restarts** |
| `API_KEYS` | apitest w/ both scopes | JSON `{key: [scopes]}` |
| `SEED_PRODUCTS` | `true` | seeds only an empty catalog |
| `RATE_LIMIT_RPM` | `300` | `0` disables |
| `BODY_LIMIT_BYTES` | `1048576` | |

## Project layout

```
cmd/server        API entrypoint (fail-fast wiring, graceful shutdown, -healthcheck)
cmd/indexer       offline corpus → index builder (the scale story)
cmd/seedredis     loads the built index into Redis (VALIDATOR=redis)
cmd/seedproducts  inserts the catalog via the public POST /api/product endpoint
internal/coupon   normalize, index format, 2-pass builder, validators
internal/api      Fiber router, middleware (auth/scopes, logging), handlers
internal/service  business rules (error taxonomy, batched lookups)
internal/store    interfaces + memory & postgres implementations
internal/seed     embedded default catalog (spec's "Chicken Waffle" = id 10)
deploy/, render.yaml, .github/workflows/ci.yml, docker-compose.yml
```

## Tests & CI

`make race` — 64 tests including the full edge-case matrix over a real index
file, the indexer end-to-end on a miniature corpus replicating the real
trap structure (codes planted at EOF, near-miss single-file codes,
duplicate-within-file, truncated-gzip hard failure), index corruption
(bit-flip → CRC), and the padding trap. CI (GitHub Actions) runs fmt, vet,
race tests, benchmark, Docker build, and a **container smoke test that
validates HAPPYHRS/SUPER100 against the real index inside the image**.

## Deploying (Render)

[render.yaml](render.yaml) is a Blueprint: web service (this Dockerfile,
health-gated on `/readyz`) + managed Postgres wired via `DATABASE_URL`.
Dashboard → Blueprints → New → point at this repo. Free-tier note: Render's
free Postgres expires after 30 days; set `STORE=memory` for an infra-free
demo deployment.

## Notes for reviewers

- The hosted demo API and docs page were sunset with Deno Deploy Classic
  (July 2026); the pinned [api/openapi.yaml](api/openapi.yaml) is the source
  of truth, per the assignment email.
- The spec types the product path id as int64 while `Product.id` is a string
  ("10") — handled as: strict integer parsing of the path (400 otherwise),
  canonical string ids in responses.
- Product reads are public because the spec declares security only on
  `POST /order`.
- Unknown JSON fields are tolerated (forward compatibility); known fields
  with wrong types are rejected (400). Both choices are deliberate and tested.

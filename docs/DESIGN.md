# Design

This document explains the architecture decisions, with the measurements that
back them. The short version: **coupon validity is a property of the corpus,
not the request — so it is computed once, offline, and served from memory.**

## The problem hiding inside the assignment

The API surface is small (2 read endpoints, 1 write). The real problem is the
promo-code rule:

> A code is valid iff it is 8–10 characters long AND appears in at least 2 of
> 3 files — where the files are 2.1 GB gzipped / ~3.1 GB raw, totalling
> **313,064,705 lines**.

That is a **data-processing problem with an API in front of it**. Checking
membership at request time — by scanning files, or by querying 313M rows
loaded into a database — puts batch work on the money path. We split it
instead:

```
BUILD TIME (once per corpus)                     REQUEST TIME (every order)
couponbase{1,2,3}.gz ──► cmd/indexer ──► coupons.idx ──► loaded at startup ──► 18ns lookup
   3.1 GB raw               ~66 s           736 B            once                 0 allocs
```

## Measured numbers (Apple M4 Pro, 14 cores; reproducible via `make index`)

| Metric | Value |
|---|---|
| Corpus | 313,064,705 lines, 2.1 GB gz / ~3.1 GB raw |
| Full index build | **65.7 s** (pass 1: 11.6 s, pass 2: 41.4 s, pass 3: 12.7 s) |
| Streaming throughput | 12.1 M lines/s/core (83 ns/line) |
| Valid codes found | **8** (exactly; verified two independent ways) |
| Index artifact | **736 bytes** |
| Runtime lookup | **18.3 ns/op, 0 allocs** (`make bench`) |
| Server RAM for coupons | ~1 KB |

## The 3-pass indexer (`internal/coupon/builder.go`)

1. **Scatter** — stream each gz (constant memory), `normalize()` every line
   (trim; 8–10 bytes; `[A-Z0-9]` only), hash survivors (FNV-1a 64), append the
   hash to 1 of 256 partition spill files routed by the hash's top byte.
   Same code ⇒ same hash ⇒ same partition, from any file — so cross-file
   matching never needs more than 1/256 of the data in memory.
2. **Count** — per partition: bitmask per hash of which files contain it
   (a code appearing twice in one file sets the same bit — the "counts once
   per file" rule falls out of the representation). Popcount ≥ 2 ⇒ candidate.
3. **Verify** — hashes can collide, and coupons are money, so candidates are
   re-resolved to **exact strings** by re-streaming the corpus; the ≥2-files
   rule is re-applied on real strings. A pass-1 collision can only *add* a
   candidate (superset), and pass 3 removes every false one — the result is
   **provably exact, not probabilistic**.

**Failure policy: abort on any read error.** All 8 valid codes physically sit
in the *last lines* of their files. A truncated download that is silently
tolerated produces an index that rejects every valid coupon with no error
anywhere. So the builder checks `scanner`/gzip errors on every read, verifies
CRC by draining to EOF, records source sha256s into the index header, and
writes atomically (same-dir temp → fsync → rename). No partial index can ever
exist. `TestBuildFailsOnTruncatedGzip` locks this in.

**Index format** (`coupons.idx`): magic + count + CRC32 + JSON provenance
(source names/sizes/sha256s/line counts) + fixed-width 10-byte records,
**NUL-padded** (never ASCII `'0'`: `'0'` is a legal code character, and
'0'-padding would make valid `OVER9000` byte-identical to the invalid
10-char input `OVER900000` — a false accept; `TestIndexPaddingUnambiguous`).
Records are globally sorted; lookup is a zero-parse binary search.

**One `normalize()`** is shared by the indexer (passes 1 & 3) and the API
request path, so the two can never disagree about what a code is.

## Why not X — the alternatives, honestly

**Why not scan the files per request?** 30–60 s and all cores per check.
Dead at any traffic.

**Why not load all codes into a database (the raw-load design)?** Each code
becomes a row with tens of bytes of overhead plus an index: 313M rows ≈
20–60 GB of storage for ~3 GB of text whose useful content is 8 codes.
Initial load is hours of bulk upserts. Every restart or new environment pays
it again. And the request path gains a network hop plus a failure domain.
The stored thing is the *input*; the right thing to store is the *answer*.

**Why not Redis?** Same analysis in two flavors:
- *Raw codes in Redis* (`EXISTS f1:CODE f2:CODE f3:CODE` ≥ 2 — a clean idea):
  ~60–100 B/key overhead ⇒ **20–30 GB of RAM** to represent 3 GB of text;
  minutes of pipelined loading per environment; multi-GB persistence files.
- *Precomputed set in Redis* (`SISMEMBER valid CODE`): correct and shipped
  here as `VALIDATOR=redis` — but for immutable data it only adds a ~100–500 µs
  network hop and a new "Redis is down" failure mode over the in-process
  index. It becomes the right answer the day coupons acquire **runtime
  state** (usage limits, live activation, per-user rules) — which is why the
  seam exists.

**Why not Postgres for coupons?** Right tool for *orders* (money wants ACID —
`STORE=postgres` writes order + items in one transaction). Wrong tool for a
read-only membership set of 8 entries.

**Decision rule** (answers every variant of these questions): *does the
request path need data that changes at runtime and must be shared across
nodes? No → ship it with the process. Yes → external store.*

## What breaks this design — stated before you ask

| Change | Response |
|---|---|
| Corpus grows 10–100× | Only the batch job feels it; the 256-partition scatter/count is a single-node MapReduce and lifts to a distributed shuffle unchanged. Serving is untouched (it depends on answer size, not corpus size). |
| Valid set grows to ~GBs | `mmap` the index from a read-only volume (same format, binary search on pages) — pods on a node share page cache. Beyond that: a dedicated coupon service sharded by the same hash. |
| Coupons become mutable / live | The immutability premise dies ⇒ swap the `Validator` implementation to Redis/DB — the seam is already in the code and README-documented. |
| Freshness in seconds required | Artifact pipelines are the wrong tool; move to streaming ingest + shared store. Nothing in the assignment requires this. |

## Runtime architecture

```
Fiber (v2) HTTP ── middleware: requestid → recover → logging → [rate limit] → auth(scopes)
      │
      ├── GET  /api/product, /api/product/:id      (public, per spec)
      ├── POST /api/order      requires create_order scope
      ├── POST /api/product    requires manage_products scope (extension)
      ├── GET  /healthz /readyz /openapi.yaml
      │
   services (validation, orchestration)
      │
      ├── store.ProductStore / store.OrderStore ── memory (default) | postgres (pgx, tx)
      └── coupon.Validator ── index (default) | redis | static (dev-only fixture)
```

- **Fail-fast startup**: missing/corrupt index (bad magic, size, CRC) ⇒ the
  process exits with an actionable message. `/readyz` passes only when every
  dependency answers and reports *which* index version is live (build time,
  source checksums, code count) — "which coupon truth is this pod serving?"
  is always answerable.
- **Fail closed at runtime**: if a remote validator (Redis) errors, orders
  carrying coupons get 503 — an unreachable backend never approves a discount
  and never silently rejects a valid one.
- **Graceful shutdown**: SIGTERM → stop accepting → drain in-flight (10 s
  budget) → exit. Zero dropped orders on deploys.
- **No N+1**: order items resolve through one batched `GetMany`.
- Bounded body (1 MiB), read/write/idle timeouts, per-IP rate limit,
  panic recovery, structured logs with request ids, api_key never logged.

## Scale-out story

The server is stateless (with `STORE=postgres`); every instance carries its
own complete copy of the 736-byte coupon truth, so replicas share nothing and
scaling is horizontal behind any LB. Corpus updates: rebuild offline → new
artifact version → rolling restart; readiness gating means a bad index stalls
the rollout while old pods keep serving. PACELC: the coupon path has no
partition sensitivity at all (no shared runtime state) and minimum latency;
order writes choose consistency (refuse orders you cannot durably record).

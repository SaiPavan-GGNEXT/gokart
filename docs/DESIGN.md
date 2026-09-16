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
   3.1 GB raw               ~16 s           736 B            once                 0 allocs
```

## Measured numbers (Apple M4 Pro, 14 cores; reproducible via `make index`)

| Metric | Value |
|---|---|
| Corpus | 313,064,705 lines, 2.1 GB gz / ~3.1 GB raw |
| Full index build | **16.2 s** (pass 1: 11.5 s — gzip-bound; pass 2: 4.6 s on 14 workers) |
| Streaming throughput | 12.1 M lines/s/core (83 ns/line) |
| Valid codes found | **8** (exactly; verified two independent ways) |
| Index artifact | **736 bytes** |
| Runtime lookup | **18.3 ns/op, 0 allocs** (`make bench`) |
| Server RAM for coupons | ~1 KB |

## The 2-pass indexer (`internal/coupon/builder.go`)

1. **Scatter** — stream each gz (constant memory), `normalize()` every line
   (trim; 8–10 bytes; `[A-Z0-9]` only), and spill **the code itself** as a
   fixed-width NUL-padded 10-byte record into 1 of 256 partition files,
   routed by the top byte of the code's FNV-1a hash. Same code ⇒ same hash ⇒
   same partition, from any file — so cross-file matching never needs more
   than 1/256 of the data in memory. The hash is used *only* for routing;
   a hash collision merely co-locates two different codes in one partition,
   where the byte-wise comparison still tells them apart.
2. **Count** — per partition (concurrently across a worker pool): sort the
   records from all files, then OR a per-file bit across each run of equal
   codes (a code appearing twice in one file sets the same bit — the "counts
   once per file" rule falls out of the representation). Popcount ≥ 2 ⇒ the
   code is valid — as an **exact string, by construction**. No probabilistic
   structure exists anywhere in the pipeline.

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

**One `normalize()`** is shared by the indexer and the API request path, so
the two can never disagree about what a code is.

### Complexity, optimality, and the measured evolution

Time is **O(N·log(N/P))** — a linear scatter over N lines plus a comparison
sort inside each of the P partitions. Memory is **O(Workers × N/P)**,
independent of corpus size. The lower bound for the problem is **Ω(N)**:
every line must be examined (any skipped line could be a valid code), and
gzip input additionally forces decompressing every byte — ~11.5 s on this
machine, which is the physical floor. The remaining log factor is the
per-partition sort; an LSD radix sort over the record bytes would remove it,
worth ≲3 s and noted as the next step rather than taken.

The pipeline got here in three measured steps, each verified to produce a
byte-identical index (same CRC-32, same 8 codes):

| Version | Pipeline | Full-corpus build |
|---|---|---|
| v1 | 3 passes (hash-scatter → count → string-verify), sequential pass 2 | 65.7 s |
| v1.1 | pass 2 parallelized across a worker pool (14 workers) | 28.4 s |
| **v2 (current)** | **2 passes: scatter the codes themselves; count exact strings** | **16.2 s** |

The v1→v1.1 lesson was about hardware: the 256 partitions are independent by
construction, yet were counted on one core — 41 s of the build with 13 cores
idle. `forEachPartition` (work-stealing off an atomic counter, results merged
in partition order for determinism) fixed that.

The v1.1→v2 lesson was about design: v1 spilled 8-byte *hashes* to save
~600 MB of scratch disk, which forced a third full pass — 12.7 s of repeated
gzip decompression — to resolve candidate hashes back to exact strings and
rule out collisions. Spilling the 10-byte *code itself* makes the count exact
by construction: the collision question disappears rather than being
answered, an entire pass is deleted, and the algorithm is *simpler* than the
one it replaced. Cost: ~25 % more temp disk (3.1 GB vs 2.5 GB, deleted after
the build). Records are packed as big-endian (uint64, uint16) pairs so the
per-partition sort runs on integer comparisons while preserving byte order.

After v2, ~11.5 s of the 16.2 s is gzip decompression — within ~5 s of the
Ω-floor for this input format. Going lower means changing the input, not the
algorithm: block-indexed compression (bgzf/`pigz -i`) or zstd would allow
parallel decompression; a machine with ~32 GB free RAM could instead use a
single hash map in one pass. Both are corpus-pipeline decisions, not code.

## The catalog read path (implemented)

The menu is the read-heavy surface, so it gets the same treatment as the
coupon index — **the database is not on the read hot path**:

```
customer reads ──► pod-local snapshot (atomic.Pointer, zero I/O, lock-free)
                        ▲ rebuilt every CATALOG_REFRESH_INTERVAL (default 2m)
                        │ by a background refresher that reads from a REPLICA
writes (POST /product, orders) ─────────────────────────► PRIMARY
                                        └─ streaming replication ─► REPLICA(s)
```

Three cooperating pieces, each independently defensible:

- **Snapshot cache** (`internal/store/cached`): refresh-ahead, so reads never
  pay a cache-miss and refreshes can't stampede; a cheap fingerprint
  (count + max id — the table is insert-only) skips reloads when nothing
  changed; **write-through** gives read-your-writes on the writing instance;
  **fail-static** keeps serving the last snapshot through a database outage.
  Staleness is bounded by the interval — menu-appropriate. With N instances,
  total DB read load is N fingerprint queries per interval, independent of
  traffic. This is the repo's one data pattern a third time: immutable
  snapshot + background refresh + atomic swap (coupon index, corpus watcher,
  now catalog).
- **Read/write split** (`internal/store/postgres`): reads round-robin over
  `DATABASE_REPLICA_URL` pools; writes, order transactions, and schema
  always use the primary. Unconfigured, both roles share one pool — zero
  behavior change. Orders never read from replicas: money follows strong
  consistency, menus tolerate eventual — per-data-type, never blanket.
- **A real replication demo** (`docker compose --profile replica up`,
  port 8085): postgres primary + streaming hot standby (pg_basebackup -R),
  API wired to both. Verified end-to-end: a row inserted directly on the
  primary via psql appears on the replica within a second and in the API
  within one refresh tick — proving reads flow primary → replica →
  snapshot. The demo also surfaced a genuine cold-boot race (the snapshot's
  first load can query a fresh replica before the schema DDL replays —
  "relation does not exist"), fixed with a bounded retry in the cache's
  initial load. A mocked test would never have found it.

## Why not X — the alternatives, honestly

**Why not scan the files per request?** 30–60 s and all cores per check.
Dead at any traffic.

**Why not Bloom filters (one per file, valid = hits ≥2)?** The
memory-bounded classic, and worth taking seriously: ~128 MB per filter at
1 % FPR for 107 M entries, ~384 MB total, no offline step. Rejected on the
false-accept math: the dangerous case is not a random code (needs ≥2 false
positives, ≈ 3·p² ≈ 0.03 %) but a code genuinely present in **exactly one
file** — it truly hits its own filter and needs only one false positive
from the remaining two: 1 − (1−p)² ≈ **2 % at p = 0.01**. That is precisely
the class the assignment's own invalid examples (`SUPER100`, `MOODYHRS`)
belong to, and the corpus contains ~313 M codes of it; with fixed hash
seeds each one is a frozen lottery ticket for a free discount. The exact
index makes the category impossible — and costs *less* on every axis
anyway: ~1 KB of RAM instead of 384 MB, millisecond startup instead of a
per-node build, and 18 ns lookups instead of 21 hash probes. Bloom filters
earn their keep when the exact set cannot be precomputed or held; here it
can, so probabilistic buys nothing and risks money.

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

## Updating the corpus whenever you like (implemented)

The offline step does not mean *manual*. The intended pipeline splits raw
data from the served artifact:

```
upload anytime ─► s3://bucket/raw/couponbase*.gz
                        │   (trigger: cron / CI / S3 event / by hand)
                        ▼
              indexer job, anywhere (~16 s)
                        ▼
              s3://bucket/index/coupons.idx      (736 B, versioned, CRC'd)
                        ▼
   every server: COUPON_INDEX=<that URL>  +  COUPON_RELOAD_INTERVAL=60s
                        ▼
        polls with If-None-Match; on change: fetch → full verification
        (magic/size/CRC) → atomic in-memory swap. Zero restarts.
```

Semantics (`internal/coupon/remote.go`, tested in `remote_test.go`):
**initial fetch is fail-fast** (no verified index, no server — same policy as
a local file); **reloads are fail-static** (a bad or unreachable update is
logged and the current index keeps serving — yesterday's verified truth
beats an unverifiable update); **swaps are atomic** (`atomic.Pointer` —
requests see old or new, never a mix, no locks on the lookup path).

The trigger side is implemented too: **`cmd/watcher`** (a sidecar, never
part of the serving process) fingerprints the raw sources — local paths by
size+mtime, URLs by one HEAD each (ETag/Last-Modified/Length) — and on
change waits a **settle window**, re-fingerprints, and only rebuilds once
the set has stopped moving: a multi-file corpus upload is only consistent
as a *set*, and uploads are not atomic across files (the torn-upload guard,
`TestWatcherTornUploadGuard`). It then runs the exact 2-pass build and
publishes by atomic rename (local) or HTTP PUT (S3/MinIO). `-once` makes
the same binary the CronJob payload or an S3-event handler. Verified live
end-to-end: touch a real corpus file → detect → settle → 16 s rebuild of
313M lines → PUT to MinIO → serving API hot-swaps on its next poll, no
restarts. Processing stays out of the serving process because serving
needs megabytes and nanoseconds while rebuilds need gigabytes and seconds:
fusing them sizes every replica for the batch job, multiplies rebuilds by
the replica count (or demands leader election in a stateless service), and
puts live traffic inside the batch blast radius.

Why not process the raw files at pod startup or during `docker build`
instead? Cadence mismatch: code changes often, the corpus changes rarely.
Startup processing makes every replica pay download + build on every boot
(and couples booting to S3 being up); build-time processing makes every CI
run and deploy pay it (and Docker layer caches silently serve stale corpora).
Binding the work to its own trigger — a corpus change — keeps pods booting
in milliseconds and builds hermetic, while freshness = pipeline run + one
poll interval.

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

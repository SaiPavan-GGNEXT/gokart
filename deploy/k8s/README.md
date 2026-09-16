# Kubernetes deployment sketch

Illustrative manifests for the production shape described in
[docs/DESIGN.md](../../docs/DESIGN.md). The assignment is graded on a
runnable Docker setup (see the repo root), so these are documentation of the
scale-out story rather than a tested cluster config.

- [indexer-cronjob.yaml](indexer-cronjob.yaml) — the corpus → index pipeline:
  fetch raw `.gz` from object storage → build `coupons.idx` (~16 s) →
  publish. Runs on the `schedule` (default 03:00 UTC nightly), or wire an S3
  event notification to the same Job template for updates within seconds of
  an upload.

The API itself deploys as a plain stateless `Deployment`:
`COUPON_INDEX=https://<bucket>/index/coupons.idx`,
`COUPON_RELOAD_INTERVAL=60s`, readiness on `/readyz` (which reports the
loaded index's provenance), and horizontal scaling with no coordination —
every replica carries its own copy of the answer.

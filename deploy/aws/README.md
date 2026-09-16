# S3 event-driven index pipeline

The event-driven variant of the corpus pipeline: instead of a cron cadence,
the index rebuild fires **seconds after new coupon files land in the
bucket**. End-to-end freshness ≈ upload → event (~seconds) → build (~16 s) →
publish → servers' `COUPON_RELOAD_INTERVAL` poll (≤60 s): **under two
minutes from "marketing uploads a file" to "every server serves the new
truth", with zero deploys or restarts.**

```
aws s3 cp couponbase2.gz  s3://bucket/raw/          (whenever you like)
aws s3 cp _complete       s3://bucket/raw/_complete (marker LAST — see below)
        │
        ▼  S3 Event Notification (filter: raw/_complete)
   trigger (Lambda / SQS→Job)
        │
        ▼  fetch raw/*.gz → /app/indexer → verify → upload
   s3://bucket/index/coupons.idx        (736 B, CRC'd, provenance header)
        │
        ▼  servers poll with If-None-Match, hot-swap atomically
   fleet serves the new truth; /readyz shows the new build stamp
```

## The torn-upload trap (why the `_complete` marker exists)

A corpus update is **three files**, and S3 fires an event per object — so a
naive "trigger on any `raw/*.gz`" builds while the upload is half done,
producing an index from one new file and two old ones. Same disease as the
truncated-download problem the indexer hard-fails on, one level up: the
unit of consistency is the *file set*, not the file. Standard cure: the
uploader writes a marker object (or a manifest listing the set + checksums)
**after** all data files, and the event notification filters on the marker
alone. The indexer's per-file sha256s recorded in the index header then
prove which corpus any index came from.

Everything downstream is safe by construction anyway: the build is
idempotent (same inputs → byte-identical output, CRC-verified), a failed
build publishes nothing, and `index/coupons.idx` is replaced atomically by
S3 (PUT is atomic per object) — servers see old or new, never torn.

## Variant 1 — S3 → Lambda (serverless, least moving parts)

Package the indexer as a container-image Lambda (~10 GB ephemeral storage
covers 2.1 GB raw + ~3.1 GB spill; at 10 GB memory Lambda grants ~6 vCPUs,
so the build runs well under a minute). Sketch:

```bash
# Notification: fire only on the completion marker
aws s3api put-bucket-notification-configuration --bucket $BUCKET \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [{
      "LambdaFunctionArn": "arn:aws:lambda:...:function:coupon-indexer",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {"Key": {"FilterRules": [
        {"Name": "prefix", "Value": "raw/"},
        {"Name": "suffix", "Value": "_complete"}]}}
    }]}'
```

The function body is exactly the CronJob's three steps (fetch → `/app/indexer`
→ publish) in a Lambda handler.

## Variant 2 — S3 → SQS → Kubernetes Job (fits the k8s story)

S3 notification → SQS queue → [KEDA `ScaledJob`](https://keda.sh) scaling on
queue depth spawns the same Job template as
[../k8s/indexer-cronjob.yaml](../k8s/indexer-cronjob.yaml) (swap the
`schedule` trigger for the queue trigger). Choose this when the batch
infrastructure already lives in the cluster; choose Lambda when it doesn't.

## Honesty notes

- These manifests/commands are **documentation-grade**: the assignment's
  corpus lives in a bucket we do not own, and event notifications can only
  be configured by the bucket owner. In a production setup you own the
  bucket, and this wiring is routine.
- Keep the cron as a **backstop even with events** (belt and braces): a
  nightly idempotent rebuild costs 16 s and heals any missed event.

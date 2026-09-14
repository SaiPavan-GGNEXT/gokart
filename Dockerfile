# Multi-stage build: Go toolchain → distroless static runtime.
# The committed coupon index (<1 KB, built from the full 2.1 GB corpus) is
# baked in, so the container serves real full-corpus validation instantly —
# the raw corpus is never downloaded at build or run time.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/indexer ./cmd/indexer \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/seedredis ./cmd/seedredis

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/server /out/indexer /out/seedredis /app/
COPY data/coupons.idx /app/data/coupons.idx
ENV COUPON_INDEX=/app/data/coupons.idx \
    ENV=prod
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["/app/server", "-healthcheck"]
ENTRYPOINT ["/app/server"]

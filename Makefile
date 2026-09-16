.PHONY: build test race bench run index corpus seed docker up up-redis up-postgres down fmt vet ci clean

BIN := bin

build: ## Build all binaries
	go build -o $(BIN)/server ./cmd/server
	go build -o $(BIN)/indexer ./cmd/indexer
	go build -o $(BIN)/seedredis ./cmd/seedredis

test: ## Run all tests
	go test ./... -count=1

race: ## Tests with the race detector (CI gate)
	go test ./... -race -count=1

bench: ## Coupon lookup benchmark
	go test ./internal/coupon/ -bench=. -benchmem -run=NONE

run: ## Run the API locally (memory store, committed index)
	go run ./cmd/server

corpus: ## Download the raw 2.1 GB coupon corpus into data/
	@for i in 1 2 3; do \
	  test -f data/couponbase$$i.gz || \
	  curl -L -o data/couponbase$$i.gz \
	    "https://orderfoodonline-files.s3.ap-southeast-2.amazonaws.com/couponbase$$i.gz"; \
	done

index: ## Rebuild data/coupons.idx from the raw corpus (needs `make corpus` first)
	go run ./cmd/indexer -out data/coupons.idx \
	  data/couponbase1.gz data/couponbase2.gz data/couponbase3.gz

seed: ## Insert the catalog through the public POST /api/product endpoint
	go run ./cmd/seedproducts

docker: ## Build the container image
	docker build -t kart-challenge .

up: ## Run the API in Docker (instant, no external services)
	docker compose up --build -d api

up-redis: ## Run the Redis-validator variant (port 8081)
	docker compose --profile redis up --build -d

up-postgres: ## Run the Postgres-store variant (port 8082)
	docker compose --profile postgres up --build -d

up-pipeline: ## Run the live-update variant: MinIO object store + polling API (port 8083)
	docker compose --profile pipeline up --build -d

prewarm-corpus: ## Download + sha256-verify the corpus into the livebuild volume (run before demos)
	docker compose --profile livebuild run --rm fetch-corpus

up-livebuild: ## LIVE artifact creation: fetch corpus → build index on screen → serve it (port 8084)
	docker compose --profile livebuild up --build

publish-index: ## Push data/coupons.idx into the running MinIO — the API hot-swaps it within 15s
	docker run --rm --network kart-challenge_default --entrypoint sh \
	  -v "$(PWD)/data/coupons.idx:/new.idx:ro" quay.io/minio/mc:latest \
	  -c "mc alias set local http://minio:9000 kart kart12345 >/dev/null && \
	      mc cp /new.idx local/coupons/index/coupons.idx"

down:
	docker compose --profile redis --profile postgres --profile pipeline --profile livebuild down

fmt:
	gofmt -l -w .

vet:
	go vet ./...

ci: vet race docker ## Everything CI runs

clean:
	rm -rf $(BIN) coverage.out

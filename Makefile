.PHONY: run demo test bench lint docker build clean

# ── Dev ─────────────────────────────────────────────────────────────────────
run:
	go run ./cmd/workerpool

demo:
	JOB_DELAY_MS=100 WORKER_COUNT=8 go run ./cmd/workerpool

# ── Test ────────────────────────────────────────────────────────────────────
test:
	go test -race ./...

test-v:
	go test -race -v ./...

bench:
	go test -bench=. -benchmem ./benchmarks/

# ── Quality ─────────────────────────────────────────────────────────────────
lint:
	go vet ./...
	@which staticcheck > /dev/null 2>&1 && staticcheck ./... || \
		echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"

# ── Docker ──────────────────────────────────────────────────────────────────
build:
	docker build -t go-worker-pool:latest .

docker:
	docker compose up --build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f workerpool

# ── Clean ───────────────────────────────────────────────────────────────────
clean:
	go clean ./...
	docker rmi go-worker-pool:latest 2>/dev/null || true
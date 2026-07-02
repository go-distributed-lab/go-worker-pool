# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

# Install git (needed for go mod download with some deps)
RUN apk add --no-cache git

WORKDIR /app

# Copy dependency files first for better layer caching —
# these rarely change, so Docker won't re-download modules on every build
COPY go.mod ./
RUN go mod download

# Copy source and build a fully static binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /bin/workerpool \
    ./cmd/workerpool

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
# scratch = zero OS, zero shell, zero attack surface
# Final image is just the binary + nothing else
FROM scratch

# Copy CA certs in case workers ever make HTTPS calls
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary
COPY --from=builder /bin/workerpool /workerpool

ENTRYPOINT ["/workerpool"]
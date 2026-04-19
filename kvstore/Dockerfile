FROM golang:1.23-alpine AS builder

WORKDIR /src

# Copy go mod files
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY cmd ./cmd
COPY internal ./internal
COPY scripts ./scripts

# Build static binaries
ARG CGO_ENABLED=0
RUN --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -trimpath -o /out/kvstore ./cmd/server && \
    go build -ldflags="-s -w" -trimpath -o /out/kvctl ./cmd/kvctl

FROM alpine:3.20

WORKDIR /app

# Copy binaries
COPY --from=builder /out/kvstore /usr/local/bin/kvstore
COPY --from=builder /out/kvctl /usr/local/bin/kvctl

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -S appuser -u 1001 -G appgroup && \
    chown -R appuser:appgroup /app /usr/local/bin && \
    chmod +x /usr/local/bin/kvstore /usr/local/bin/kvctl

USER appuser

EXPOSE 8001 8002 8003 9001 9002 9003

ENTRYPOINT ["kvstore"]

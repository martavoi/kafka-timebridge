# Build stage
FROM golang:1.25-alpine AS builder

# Install build dependencies (librdkafka is compiled from source bundled in the module)
RUN apk add --no-progress --no-cache gcc musl-dev

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Accept version as build argument
ARG VERSION=dev

# Build the application
RUN go build -tags musl -ldflags "-s -w -X main.version=${VERSION}" -o kafka-timebridge ./cmd

# Final stage
FROM alpine:latest

LABEL org.opencontainers.image.source="https://github.com/martavoi/kafka-timebridge"
LABEL org.opencontainers.image.description="Kafka TimeBridge — time-based Kafka consumer offset seeking"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk --no-cache add \
    ca-certificates \
    tzdata

# Create non-root user
RUN addgroup -g 1001 timebridge && \
    adduser -D -s /bin/sh -u 1001 -G timebridge timebridge

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder --chown=timebridge:timebridge /app/kafka-timebridge .

# Switch to non-root user
USER timebridge

# Expose port (if needed for health checks)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
ENTRYPOINT ["./kafka-timebridge"]
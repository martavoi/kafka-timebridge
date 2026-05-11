# Build stage
FROM golang:1.26.3-alpine3.23 AS builder

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
FROM alpine:3.23

LABEL org.opencontainers.image.source="https://github.com/martavoi/kafka-timebridge"
LABEL org.opencontainers.image.description="Kafka Timebridge — daemon for delayed Kafka delivery: consumes a scheduling topic, persists messages, and produces to destination topics at the configured time (memory, Couchbase, or MongoDB)"
LABEL org.opencontainers.image.licenses="Apache-2.0"

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

# Run the application
ENTRYPOINT ["./kafka-timebridge"]

# Kafka Timebridge

A lightweight, production-ready daemon for scheduling delayed Kafka message delivery. Send messages now, deliver them later - from minutes to months in advance.

## About

Kafka Timebridge enables sophisticated delayed message scheduling in Kafka environments. Instead of sending messages directly to destination topics, clients send them to a timebridge topic with scheduling metadata. The daemon stores messages in configurable backends and delivers them to their destination topics at the specified time.

**Key Features:**
- ⏰ Schedule message delivery from minutes to months in advance
- 🗄️ Multiple storage backends: in-memory (default) and Couchbase
- 🔧 Simple header-based scheduling interface
- ⚙️ Configurable via environment variables and CLI flags
- 🔒 SASL authentication support for secure Kafka clusters
- 📊 Structured logging with configurable levels and formats
- 🐳 Docker support with Alpine-based images
- 🔄 Graceful shutdown and error handling with exponential backoff

**Use Cases:**
- Payment reminders and notifications
- Subscription renewals and expiration alerts
- Follow-up emails and marketing campaigns
- System maintenance notifications
- Any time-delayed workflow automation

## How It Works

1. **Send**: Client sends message to timebridge topic (default: `timebridge`) with headers:
   - `X-Timebridge-When`: When to deliver (RFC3339 format, e.g., `2024-12-25T10:00:00Z`)
   - `X-Timebridge-Where`: Target topic for delivery (e.g., `user-notifications`)

2. **Store**: Timebridge daemon stores the message in configured backend until delivery time

3. **Deliver**: At specified time, message is sent to destination topic

### Message Headers

| Header | Required | Format | Example |
|--------|----------|---------|---------|
| `X-Timebridge-When` | Yes | RFC3339 timestamp | `2024-12-25T10:00:00Z` |
| `X-Timebridge-Where` | Yes | Destination topic name | `user-notifications` |

**Example Message:**
```bash
# Headers
X-Timebridge-When: 2024-12-25T10:00:00Z
X-Timebridge-Where: user-notifications
Content-Type: application/json

# Body
{"userId": 123, "message": "Your subscription expires tomorrow"}
```

## Storage Backends

### In-Memory Backend (Default)
- **Purpose**: Development, testing, and simple use cases
- **Pros**: Zero configuration, instant startup
- **Cons**: Messages lost on restart, limited by available RAM
- **Configuration**: No additional setup required

### Couchbase Backend (Recommended for Production)
- **Purpose**: Production deployments requiring persistence and reliability
- **Pros**: Persistent storage, scalable, high availability
- **Cons**: Requires Couchbase cluster setup
- **Configuration**: See Couchbase settings below

## Configuration

Configure via environment variables or CLI flags. CLI flags override environment variables.

### Core Settings

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `BACKEND` | `--backend` | `memory` | Storage backend (`memory`, `couchbase`) |
| `LOG_LEVEL` | `--log-level` | `debug` | Log level (`debug`, `info`, `warn`, `error`) |
| `LOG_FORMAT` | `--log-format` | `text` | Log format (`text`, `json`) |

### Kafka Settings

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `KAFKA_BROKERS` | `--kafka-brokers` | `localhost:9092` | Kafka broker addresses (comma-separated) |
| `KAFKA_TOPIC` | `--kafka-topic` | `timebridge` | Topic to listen for scheduled messages |
| `KAFKA_GROUP_ID` | `--kafka-group-id` | `timebridge` | Consumer group ID |
| `KAFKA_USERNAME` | `--kafka-username` | | SASL username (optional) |
| `KAFKA_PASSWORD` | `--kafka-password` | | SASL password (optional) |
| `KAFKA_SECURITY_PROTOCOL` | `--kafka-security-protocol` | `SASL_PLAINTEXT` | Security protocol |
| `KAFKA_SASL_MECHANISM` | `--kafka-sasl-mechanism` | `PLAIN` | SASL mechanism |

### Couchbase Settings

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `COUCHBASE_CONNECTION_STRING` | `--couchbase-connection-string` | `couchbase://localhost` | Couchbase cluster connection string |
| `COUCHBASE_BUCKET` | `--couchbase-bucket` | `timebridge` | Bucket name for message storage |
| `COUCHBASE_SCOPE` | `--couchbase-scope` | `timebridge` | Scope within the bucket |
| `COUCHBASE_COLLECTION` | `--couchbase-collection` | `messages` | Collection for storing messages |
| `COUCHBASE_USERNAME` | `--couchbase-username` | `timebridge` | Couchbase username |
| `COUCHBASE_PASSWORD` | `--couchbase-password` | | Couchbase password |

#### Couchbase Setup Example

```bash
# 1. Start Couchbase (using Docker)
docker run -d --name couchbase \
  -p 8091-8097:8091-8097 \
  -p 9123:9123 \
  -p 11207:11207 \
  -p 11210:11210 \
  -p 11280:11280 \
  -p 18091-18097:18091-18097 \
  couchbase:latest

# 2. Configure Couchbase cluster via Web UI (http://localhost:8091)
# 3. Create bucket named 'timebridge'
# 4. Create scope named 'timebridge' within the bucket
# 5. Create collection named 'messages' within the scope

# 6. Run timebridge with Couchbase backend
BACKEND=couchbase \
COUCHBASE_CONNECTION_STRING=couchbase://localhost \
COUCHBASE_USERNAME=Administrator \
COUCHBASE_PASSWORD=password \
./kafka-timebridge
```

## Usage Examples

### Basic Usage (In-Memory)
```bash
# Default in-memory backend
./kafka-timebridge

# With custom Kafka settings
KAFKA_BROKERS=broker1:9092,broker2:9092 \
LOG_LEVEL=info \
./kafka-timebridge
```

### Production Usage (Couchbase)
```bash
# Production setup with Couchbase and SASL authentication
BACKEND=couchbase \
KAFKA_BROKERS=prod-broker1:9092,prod-broker2:9092 \
KAFKA_USERNAME=timebridge-user \
KAFKA_PASSWORD=secure-password \
KAFKA_SECURITY_PROTOCOL=SASL_SSL \
COUCHBASE_CONNECTION_STRING=couchbases://cb1.example.com,cb2.example.com \
COUCHBASE_USERNAME=timebridge \
COUCHBASE_PASSWORD=cb-password \
LOG_LEVEL=info \
LOG_FORMAT=json \
./kafka-timebridge
```

### CLI Flag Usage
```bash
# Using CLI flags (override environment variables)
./kafka-timebridge \
  --backend=couchbase \
  --kafka-brokers=localhost:9092 \
  --kafka-topic=delayed-messages \
  --log-level=debug \
  --couchbase-connection-string=couchbase://localhost
```

## Building and Running

### Local Build
```bash
# Build binary
go build -o kafka-timebridge cmd/main.go

# Run with environment variables
KAFKA_BROKERS=localhost:9092 ./kafka-timebridge

# Run directly with Go
go run cmd/main.go
```

### Docker

#### Building Docker Image
```bash
# Build image
docker build -t kafka-timebridge:latest .

# Run with in-memory backend
docker run --rm \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  kafka-timebridge:latest

# Run with Couchbase backend
docker run --rm \
  -e BACKEND=couchbase \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  -e COUCHBASE_CONNECTION_STRING=couchbase://host.docker.internal \
  -e COUCHBASE_USERNAME=Administrator \
  -e COUCHBASE_PASSWORD=password \
  kafka-timebridge:latest
```

#### Docker Compose Example
```yaml
version: '3.8'
services:
  timebridge:
    build: .
    environment:
      - BACKEND=couchbase
      - KAFKA_BROKERS=kafka:9092
      - COUCHBASE_CONNECTION_STRING=couchbase://couchbase
      - COUCHBASE_USERNAME=Administrator
      - COUCHBASE_PASSWORD=password
      - LOG_LEVEL=info
      - LOG_FORMAT=json
    depends_on:
      - kafka
      - couchbase
```

## Sending Test Messages

### Using Kafka Console Producer
```bash
# Send a delayed message
echo 'X-Timebridge-When=2024-12-25T10:00:00Z|X-Timebridge-Where=user-notifications|{"userId": 123, "message": "Test delayed message"}' | \
kafka-console-producer.sh \
  --topic timebridge \
  --bootstrap-server localhost:9092 \
  --property "parse.headers=true" \
  --property "headers.delimiter=|" \
  --property "headers.separator="
```

### Programmatic Example (Go)
```go
// Example using confluent-kafka-go
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Headers: []kafka.Header{
        {Key: "X-Timebridge-When", Value: []byte("2024-12-25T10:00:00Z")},
        {Key: "X-Timebridge-Where", Value: []byte("user-notifications")},
        {Key: "Content-Type", Value: []byte("application/json")},
    },
    Value: []byte(`{"userId": 123, "message": "Scheduled notification"}`),
}, nil)
```

## Monitoring and Logging

Timebridge provides structured logging with configurable levels and formats:

- **Log Levels**: `debug`, `info`, `warn`, `error`
- **Log Formats**: `text` (human-readable), `json` (machine-readable)
- **Key Metrics**: Message processing, backend operations, Kafka events

### Log Output Example (JSON)
```json
{"time":"2024-01-01T10:00:00Z","level":"INFO","msg":"Received scheduled message","when":"2024-12-25T10:00:00Z","where":"user-notifications","offset":123}
```

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

```mermaid
graph TB
    Producer[📱 Producer Application<br/>Schedules messages]
    
    subgraph "Kafka Cluster"
        TimebridgeTopic[📨 timebridge<br/>Kafka Topic]
        UserTopic[📬 user-notifications<br/>Kafka Topic]
        PaymentTopic[💰 payment-reminders<br/>Kafka Topic] 
    end
    
    subgraph "Timebridge System"
        Daemon[🔄 kafka-timebridge daemon]
        Backend[(🗄️ Couchbase Backend)]
    end
    
    UserConsumer[👤 User Service<br/>Consumes notifications]
    PaymentConsumer[💳 Payment Service<br/>Consumes reminders]
    
    Producer -->|"Send with headers:<br/>X-Timebridge-When<br/>X-Timebridge-Where"| TimebridgeTopic
    TimebridgeTopic -->|Consume scheduled messages| Daemon
    Daemon -->|Store until delivery time| Backend
    Backend -->|Read ready messages| Daemon
    Daemon -->|"Deliver at scheduled time<br/>(no scheduling headers)"| UserTopic
    Daemon -->|"Deliver at scheduled time<br/>(no scheduling headers)"| PaymentTopic
    
    UserTopic -->|"Consume regular messages<br/>(no time tracking needed)"| UserConsumer
    PaymentTopic -->|"Consume regular messages<br/>(no time tracking needed)"| PaymentConsumer
    
    style Producer fill:#e1f5fe
    style Daemon fill:#f3e5f5
    style Backend fill:#e8f5e8
    style TimebridgeTopic fill:#fff3e0
    style UserTopic fill:#fff3e0
    style PaymentTopic fill:#fff3e0
    style UserConsumer fill:#e8f5e8
    style PaymentConsumer fill:#e8f5e8
```

### Process Flow

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

### Programmatic Example

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





## Monitoring and Logging

Timebridge provides structured logging with configurable levels and formats:

- **Log Levels**: `debug`, `info`, `warn`, `error`
- **Log Formats**: `text` (human-readable), `json` (machine-readable)
- **Key Metrics**: Message processing, backend operations, Kafka events

### Log Output Example (JSON)
```json
{"time":"2024-01-01T10:00:00Z","level":"INFO","msg":"Received scheduled message","when":"2024-12-25T10:00:00Z","where":"user-notifications","offset":123}
```

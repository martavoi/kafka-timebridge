package timebridge

import (
	"context"
	"time"
)

type Header struct {
	Key   string
	Value []byte
}

type Message struct {
	Key     []byte
	Value   []byte
	Headers []Header
	When    time.Time
	Where   string
}

type StoredMessage struct {
	Message
	Key string
}

type BackendWriter interface {
	Write(ctx context.Context, message Message) (*StoredMessage, error)
}

type BackendReader interface {
	// ReadBatch returns messages that are ready to be delivered (when <= now)
	// ordered by 'When' field in ascending order (oldest first), limited by the specified count
	ReadBatch(ctx context.Context, limit int) ([]StoredMessage, error)
}

type BackendDeleter interface {
	Delete(ctx context.Context, key string) error
}

type BackendCloser interface {
	Close() error
}

type Backend interface {
	BackendWriter
	BackendReader
	BackendDeleter
	BackendCloser
}

// Backend type constants
const (
	BackendMemory    = "memory"
	BackendCouchbase = "couchbase"
	BackendMongoDB   = "mongodb"
)

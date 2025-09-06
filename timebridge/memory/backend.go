package memory

import (
	"context"
	"kafka-timebridge/timebridge"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Backend implements an in-memory storage backend for timebridge messages
type Backend struct {
	mu    sync.RWMutex
	store map[string]timebridge.Message
}

// NewBackend creates a new in-memory backend
func NewBackend() *Backend {
	return &Backend{
		store: make(map[string]timebridge.Message),
	}
}

// Write stores a message in memory and returns the stored message with generated key
func (b *Backend) Write(ctx context.Context, message timebridge.Message) (*timebridge.StoredMessage, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Generate unique key for the message
	key := uuid.New().String()

	// Store the message
	b.store[key] = message

	return &timebridge.StoredMessage{
		Message: message,
		Key:     key,
	}, nil
}

// ReadBatch returns messages that are ready to be delivered (when <= now) ordered by 'When' field in ascending order (oldest first)
func (b *Backend) ReadBatch(ctx context.Context, limit int) ([]timebridge.StoredMessage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	now := time.Now()

	// Convert map to slice, filtering by time
	messages := make([]timebridge.StoredMessage, 0, len(b.store))
	for key, message := range b.store {
		// Only include messages that are ready to be delivered
		if message.When.Before(now) || message.When.Equal(now) {
			messages = append(messages, timebridge.StoredMessage{
				Message: message,
				Key:     key,
			})
		}
	}

	// Sort by When field in ascending order (oldest first for FIFO processing)
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Message.When.Before(messages[j].Message.When)
	})

	// Apply limit
	if limit > 0 && limit < len(messages) {
		messages = messages[:limit]
	}

	return messages, nil
}

// Delete removes a message from memory storage
func (b *Backend) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.store, key)
	return nil
}

// Clear removes all messages from memory storage
func (b *Backend) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.store = make(map[string]timebridge.Message)
}

// Close implements the BackendCloser interface (no-op for memory backend)
func (b *Backend) Close() error {
	// Memory backend doesn't need cleanup, but we implement the interface
	// for consistency and future extensibility
	return nil
}

// Len returns the number of stored messages
func (b *Backend) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.store)
}

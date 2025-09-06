package memory

import (
	"context"
	"kafka-timebridge/timebridge"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_WriteReadDelete(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	// Test message with realistic headers (what acceptor would send)
	originalMessage := timebridge.Message{
		Key:   []byte("order-12345"),
		Value: []byte(`{"orderId": 12345, "amount": 99.99, "userId": "user123"}`),
		Headers: []timebridge.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "correlation-id", Value: []byte("corr-abc-123")},
			{Key: "source-system", Value: []byte("order-service")},
		},
		When:  time.Now().Add(30 * time.Minute), // Realistic future time
		Where: "payment-notifications",          // Realistic destination topic
	}

	t.Run("Write message", func(t *testing.T) {
		// Initially empty
		assert.Equal(t, 0, backend.Len())

		storedMessage, err := backend.Write(ctx, originalMessage)
		require.NoError(t, err, "Failed to write message")
		require.NotNil(t, storedMessage, "Stored message should not be nil")

		// Verify the stored message contains original data
		assert.Equal(t, originalMessage.Key, storedMessage.Message.Key)
		assert.Equal(t, originalMessage.Value, storedMessage.Message.Value)
		assert.Equal(t, originalMessage.Headers, storedMessage.Message.Headers)
		assert.True(t, originalMessage.When.Equal(storedMessage.Message.When), "Times should be equal: expected %v, got %v", originalMessage.When, storedMessage.Message.When)
		assert.Equal(t, originalMessage.Where, storedMessage.Message.Where)

		// Verify document key was generated
		assert.NotEmpty(t, storedMessage.Key, "Document key should be generated")

		// Verify storage count
		assert.Equal(t, 1, backend.Len())

		// Store the key for subsequent tests
		documentKey := storedMessage.Key

		t.Run("Read message", func(t *testing.T) {
			messages, err := backend.ReadBatch(ctx, 10)
			require.NoError(t, err, "Failed to read messages")
			require.Len(t, messages, 1, "Should have exactly one message")

			foundMessage := messages[0]

			// Verify document key matches
			assert.Equal(t, documentKey, foundMessage.Key)

			// Verify all fields match
			assert.Equal(t, originalMessage.Key, foundMessage.Message.Key)
			assert.Equal(t, originalMessage.Value, foundMessage.Message.Value)
			assert.Equal(t, len(originalMessage.Headers), len(foundMessage.Message.Headers))
			assert.True(t, originalMessage.When.Equal(foundMessage.Message.When), "Times should be equal: expected %v, got %v", originalMessage.When, foundMessage.Message.When)
			assert.Equal(t, originalMessage.Where, foundMessage.Message.Where)

			// Verify headers content
			if len(originalMessage.Headers) == len(foundMessage.Message.Headers) {
				for i, header := range originalMessage.Headers {
					assert.Equal(t, header.Key, foundMessage.Message.Headers[i].Key)
					assert.Equal(t, header.Value, foundMessage.Message.Headers[i].Value)
				}
			}
		})

		t.Run("Delete message", func(t *testing.T) {
			err := backend.Delete(ctx, documentKey)
			require.NoError(t, err, "Failed to delete message")

			// Verify message is deleted by trying to read it
			messages, err := backend.ReadBatch(ctx, 100)
			require.NoError(t, err, "Failed to read messages after deletion")
			assert.Empty(t, messages, "Should have no messages after deletion")

			// Verify storage count
			assert.Equal(t, 0, backend.Len())
		})
	})
}

func TestBackend_ReadBatch_Ordering(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	// Create messages with different timestamps
	now := time.Now()
	messages := []timebridge.Message{
		{
			Key:   []byte("msg1"),
			Value: []byte("first message"),
			When:  now.Add(1 * time.Hour),
			Where: "dest1",
		},
		{
			Key:   []byte("msg2"),
			Value: []byte("second message"),
			When:  now.Add(2 * time.Hour),
			Where: "dest2",
		},
		{
			Key:   []byte("msg3"),
			Value: []byte("third message"),
			When:  now.Add(3 * time.Hour),
			Where: "dest3",
		},
	}

	// Store all messages
	var documentKeys []string
	for _, msg := range messages {
		stored, err := backend.Write(ctx, msg)
		require.NoError(t, err)
		documentKeys = append(documentKeys, stored.Key)
	}

	// Verify all messages stored
	assert.Equal(t, 3, backend.Len())

	t.Run("Messages ordered by when DESC", func(t *testing.T) {
		results, err := backend.ReadBatch(ctx, 10)
		require.NoError(t, err)
		require.Len(t, results, 3, "Should find all our messages")

		// Verify ordering (DESC by when) - newest first
		assert.True(t, results[0].Message.When.After(results[1].Message.When), "First message should have later timestamp")
		assert.True(t, results[1].Message.When.After(results[2].Message.When), "Second message should have later timestamp than third")

		// Verify specific order: msg3 (3h), msg2 (2h), msg1 (1h)
		assert.Equal(t, []byte("msg3"), results[0].Message.Key)
		assert.Equal(t, []byte("msg2"), results[1].Message.Key)
		assert.Equal(t, []byte("msg1"), results[2].Message.Key)
	})

	t.Run("Limited batch size", func(t *testing.T) {
		results, err := backend.ReadBatch(ctx, 2)
		require.NoError(t, err)
		require.Len(t, results, 2, "Should respect limit")

		// Should get the two newest messages
		assert.Equal(t, []byte("msg3"), results[0].Message.Key)
		assert.Equal(t, []byte("msg2"), results[1].Message.Key)
	})

	// Clean up
	for _, key := range documentKeys {
		backend.Delete(ctx, key)
	}
}

func TestBackend_WriteWithoutKey(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	// Message without key (null key)
	messageWithoutKey := timebridge.Message{
		Key:   nil, // No key
		Value: []byte(`{"notification": "User account created", "userId": "user456"}`),
		Headers: []timebridge.Header{
			{Key: "event-type", Value: []byte("user.created")},
			{Key: "timestamp", Value: []byte("2025-09-06T15:30:00Z")},
		},
		When:  time.Now().Add(1 * time.Hour),
		Where: "user-notifications",
	}

	t.Run("Write message without key", func(t *testing.T) {
		storedMessage, err := backend.Write(ctx, messageWithoutKey)
		require.NoError(t, err, "Failed to write message without key")
		require.NotNil(t, storedMessage, "Stored message should not be nil")

		// Verify the stored message
		assert.Nil(t, storedMessage.Message.Key, "Key should remain nil")
		assert.Equal(t, messageWithoutKey.Value, storedMessage.Message.Value)
		assert.Equal(t, len(messageWithoutKey.Headers), len(storedMessage.Message.Headers))
		assert.True(t, messageWithoutKey.When.Equal(storedMessage.Message.When))
		assert.Equal(t, messageWithoutKey.Where, storedMessage.Message.Where)

		// Verify storage count
		assert.Equal(t, 1, backend.Len())

		// Cleanup
		backend.Delete(ctx, storedMessage.Key)
	})
}

func TestBackend_WriteEmptyMessage(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	// Message with empty value and no headers (minimal message)
	emptyMessage := timebridge.Message{
		Key:     []byte("empty-message-key"),
		Value:   []byte{},              // Empty value
		Headers: []timebridge.Header{}, // No headers
		When:    time.Now().Add(15 * time.Minute),
		Where:   "system-events",
	}

	t.Run("Write empty message", func(t *testing.T) {
		storedMessage, err := backend.Write(ctx, emptyMessage)
		require.NoError(t, err, "Failed to write empty message")
		require.NotNil(t, storedMessage, "Stored message should not be nil")

		// Verify the stored message
		assert.Equal(t, emptyMessage.Key, storedMessage.Message.Key)
		assert.Equal(t, emptyMessage.Value, storedMessage.Message.Value)
		assert.Empty(t, storedMessage.Message.Headers, "Headers should be empty")
		assert.True(t, emptyMessage.When.Equal(storedMessage.Message.When))
		assert.Equal(t, emptyMessage.Where, storedMessage.Message.Where)

		// Verify storage count
		assert.Equal(t, 1, backend.Len())

		// Cleanup
		backend.Delete(ctx, storedMessage.Key)
	})
}

func TestBackend_EmptyReadBatch(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	t.Run("Read from empty backend", func(t *testing.T) {
		messages, err := backend.ReadBatch(ctx, 10)
		require.NoError(t, err, "Should not error on empty backend")
		assert.Empty(t, messages, "Should return empty slice")
		assert.Equal(t, 0, backend.Len())
	})
}

func TestBackend_DeleteNonExistentKey(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	t.Run("Delete non-existent key", func(t *testing.T) {
		err := backend.Delete(ctx, "non-existent-key")
		assert.NoError(t, err, "Deleting non-existent key should not error")
		assert.Equal(t, 0, backend.Len())
	})
}

func TestBackend_ConcurrentAccess(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	t.Run("Concurrent writes", func(t *testing.T) {
		numWriters := 10
		done := make(chan struct{}, numWriters)

		for i := 0; i < numWriters; i++ {
			go func(id int) {
				defer func() { done <- struct{}{} }()

				message := timebridge.Message{
					Key:   []byte("concurrent-key"),
					Value: []byte("concurrent message"),
					When:  time.Now().Add(time.Duration(id) * time.Minute),
					Where: "concurrent-dest",
				}

				_, err := backend.Write(ctx, message)
				assert.NoError(t, err, "Concurrent write should not error")
			}(i)
		}

		// Wait for all writers to complete
		for i := 0; i < numWriters; i++ {
			<-done
		}

		// Verify all messages were stored
		assert.Equal(t, numWriters, backend.Len())

		// Clean up
		backend.Clear()
		assert.Equal(t, 0, backend.Len())
	})
}

func TestBackend_Clear(t *testing.T) {
	backend := NewBackend()
	ctx := context.Background()

	// Add some messages
	for i := 0; i < 3; i++ {
		message := timebridge.Message{
			Key:   []byte("test-key"),
			Value: []byte("test message"),
			When:  time.Now().Add(time.Duration(i) * time.Minute),
			Where: "test-dest",
		}
		_, err := backend.Write(ctx, message)
		require.NoError(t, err)
	}

	assert.Equal(t, 3, backend.Len())

	t.Run("Clear all messages", func(t *testing.T) {
		backend.Clear()
		assert.Equal(t, 0, backend.Len())

		messages, err := backend.ReadBatch(ctx, 10)
		require.NoError(t, err)
		assert.Empty(t, messages)
	})
}

// TestBackend_ImplementsInterface verifies that Backend implements all required interfaces
func TestBackend_ImplementsInterface(t *testing.T) {
	backend := NewBackend()

	// Verify it implements the timebridge.Backend interface
	var _ timebridge.Backend = backend
	var _ timebridge.BackendWriter = backend
	var _ timebridge.BackendReader = backend
	var _ timebridge.BackendDeleter = backend

	t.Log("Memory backend correctly implements all required interfaces")
}

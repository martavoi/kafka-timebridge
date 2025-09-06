package couchbase

import (
	"context"
	"kafka-timebridge/timebridge"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTestConfig returns configuration for pre-created test resources
func getTestConfig() timebridge.CouchbaseConfig {
	return timebridge.CouchbaseConfig{
		Bucket:           "timebridge-test", // Pre-created bucket
		Scope:            "timebridge",      // Pre-created scope
		Collection:       "messages",        // Pre-created collection
		Username:         "timebridge-test",
		Password:         "123456",
		ConnectionString: "couchbase://localhost",
	}
}

func TestBackend_WriteReadDelete(t *testing.T) {
	// Skip test if running in CI or no Couchbase available
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	err = backend.Connect()
	require.NoError(t, err, "Failed to connect to Couchbase")
	defer backend.Close()

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

		// Store the key for subsequent tests
		documentKey := storedMessage.Key

		t.Run("Read message", func(t *testing.T) {
			messages, err := backend.ReadBatch(ctx, 10)
			require.NoError(t, err, "Failed to read messages")

			// Find our message in the batch
			var foundMessage *timebridge.StoredMessage
			for _, msg := range messages {
				if msg.Key == documentKey {
					foundMessage = &msg
					break
				}
			}

			require.NotNil(t, foundMessage, "Should find our stored message")

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

			// Ensure our message is not in the results
			for _, msg := range messages {
				assert.NotEqual(t, documentKey, msg.Key, "Deleted message should not be found")
			}
		})
	})
}

func TestBackend_ReadBatch_Ordering(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err)

	err = backend.Connect()
	require.NoError(t, err)
	defer backend.Close()

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

	// Clean up after test
	defer func() {
		for _, key := range documentKeys {
			backend.Delete(ctx, key)
		}
	}()

	t.Run("Messages ordered by when DESC", func(t *testing.T) {
		results, err := backend.ReadBatch(ctx, 10)
		require.NoError(t, err)

		// Find our messages in results
		var ourMessages []timebridge.StoredMessage
		for _, result := range results {
			for _, key := range documentKeys {
				if result.Key == key {
					ourMessages = append(ourMessages, result)
					break
				}
			}
		}

		require.Len(t, ourMessages, 3, "Should find all our messages")

		// Verify ordering (DESC by when)
		assert.True(t, ourMessages[0].Message.When.After(ourMessages[1].Message.When), "First message should have later timestamp")
		assert.True(t, ourMessages[1].Message.When.After(ourMessages[2].Message.When), "Second message should have later timestamp than third")
	})
}

func TestBackend_WriteWithoutKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	err = backend.Connect()
	require.NoError(t, err, "Failed to connect to Couchbase")
	defer backend.Close()

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

		// Cleanup
		defer backend.Delete(ctx, storedMessage.Key)
	})
}

func TestBackend_WriteEmptyMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	err = backend.Connect()
	require.NoError(t, err, "Failed to connect to Couchbase")
	defer backend.Close()

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

		// Cleanup
		defer backend.Delete(ctx, storedMessage.Key)
	})
}

func TestBackend_WriteError_InvalidConfig(t *testing.T) {
	// Test with invalid configuration
	cfg := timebridge.CouchbaseConfig{
		Bucket:           "nonexistent-bucket",
		Scope:            "_default",
		Collection:       "_default",
		Username:         "invalid",
		Password:         "invalid",
		ConnectionString: "couchbase://nonexistent:8091",
	}

	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Backend creation should succeed")

	// Connection should fail with invalid config
	err = backend.Connect()
	assert.Error(t, err, "Connection should fail with invalid config")
}

// TestBackend_ImplementsInterface verifies that Backend implements all required interfaces
func TestBackend_ImplementsInterface(t *testing.T) {
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	// Verify it implements the timebridge.Backend interface
	var _ timebridge.Backend = backend
	var _ timebridge.BackendWriter = backend
	var _ timebridge.BackendReader = backend
	var _ timebridge.BackendDeleter = backend

	t.Log("Couchbase backend correctly implements all required interfaces")
}

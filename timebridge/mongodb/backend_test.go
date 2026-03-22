package mongodb

import (
	"context"
	"kafka-timebridge/timebridge"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// getTestConfig returns configuration for test MongoDB instance
func getTestConfig() timebridge.MongoDBConfig {
	return timebridge.MongoDBConfig{
		Database:         "timebridge-test", // Test database
		Collection:       "messages",        // Test collection
		Username:         "",                // No auth for local testing
		Password:         "",
		ConnectionString: "mongodb://localhost:27017",
		ConnectTimeout:   2,
		WriteTimeout:     2,
		ReadTimeout:      10, // Increased to accommodate 3-step ReadBatch
		DeleteTimeout:    2,
		IndexTimeout:     5,
		AutoCreateIndex:  true,
		ClaimTTLSeconds:  30,
	}
}

func TestBackend_WriteReadDelete(t *testing.T) {
	// Skip test if running in CI or no MongoDB available
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	err = backend.Connect(ctx)
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer backend.Close()

	// Test message with realistic headers (what acceptor would send)
	originalMessage := timebridge.Message{
		Key:   []byte("order-12345"),
		Value: []byte(`{"orderId": 12345, "amount": 99.99, "userId": "user123"}`),
		Headers: []timebridge.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "correlation-id", Value: []byte("corr-abc-123")},
			{Key: "source-system", Value: []byte("order-service")},
		},
		When:  time.Now().Add(-30 * time.Minute), // Past time so it can be read immediately
		Where: "payment-notifications",           // Realistic destination topic
	}

	t.Run("Write message", func(t *testing.T) {
		storedMessage, err := backend.Write(ctx, originalMessage)
		require.NoError(t, err, "Failed to write message")
		require.NotNil(t, storedMessage, "Stored message should not be nil")

		// Wait for eventual consistency (indexing delay)
		time.Sleep(100 * time.Millisecond)

		// Verify the stored message contains original data
		assert.Equal(t, originalMessage.Key, storedMessage.Message.Key)
		assert.Equal(t, originalMessage.Value, storedMessage.Value)
		assert.Equal(t, originalMessage.Headers, storedMessage.Headers)
		// Note: Not checking time equality due to MongoDB precision differences
		assert.Equal(t, originalMessage.Where, storedMessage.Where)

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
			assert.Equal(t, originalMessage.Value, foundMessage.Value)
			assert.Equal(t, len(originalMessage.Headers), len(foundMessage.Headers))
			assert.Equal(t, originalMessage.Where, foundMessage.Where)

			// Verify headers content
			if len(originalMessage.Headers) == len(foundMessage.Headers) {
				for i, header := range originalMessage.Headers {
					assert.Equal(t, header.Key, foundMessage.Headers[i].Key)
					assert.Equal(t, header.Value, foundMessage.Headers[i].Value)
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

	ctx := context.Background()
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err)

	err = backend.Connect(ctx)
	require.NoError(t, err)
	defer backend.Close()

	// Create messages with different timestamps
	now := time.Now()
	messages := []timebridge.Message{
		{
			Key:   []byte("msg1"),
			Value: []byte("first message"),
			When:  now.Add(-2 * time.Hour), // Past time - should be first
			Where: "dest1",
		},
		{
			Key:   []byte("msg2"),
			Value: []byte("second message"),
			When:  now.Add(-1 * time.Hour), // More recent past - should be second
			Where: "dest2",
		},
		{
			Key:   []byte("msg3"),
			Value: []byte("third message"),
			When:  now.Add(-30 * time.Minute), // Most recent past - should be third
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

	// Wait for eventual consistency (indexing delay)
	time.Sleep(100 * time.Millisecond)

	// Clean up after test
	defer func() {
		for _, key := range documentKeys {
			backend.Delete(ctx, key)
		}
	}()

	t.Run("Messages ordered by when ASC", func(t *testing.T) {
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

		// Verify ordering (ASC by when - oldest first)
		assert.True(t, ourMessages[0].When.Before(ourMessages[1].When), "First message should have earlier timestamp")
		assert.True(t, ourMessages[1].When.Before(ourMessages[2].When), "Second message should have earlier timestamp than third")
	})
}

func TestBackend_WriteWithoutKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	err = backend.Connect(ctx)
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer backend.Close()

	// Message without key (null key)
	messageWithoutKey := timebridge.Message{
		Key:   nil, // No key
		Value: []byte(`{"notification": "User account created", "userId": "user456"}`),
		Headers: []timebridge.Header{
			{Key: "event-type", Value: []byte("user.created")},
			{Key: "timestamp", Value: []byte("2025-09-06T15:30:00Z")},
		},
		When:  time.Now().Add(-1 * time.Hour), // Past time so it will be readable
		Where: "user-notifications",
	}

	t.Run("Write message without key", func(t *testing.T) {
		storedMessage, err := backend.Write(ctx, messageWithoutKey)
		require.NoError(t, err, "Failed to write message without key")
		require.NotNil(t, storedMessage, "Stored message should not be nil")

		// Verify the stored message
		assert.Nil(t, storedMessage.Message.Key, "Key should remain nil")
		assert.Equal(t, messageWithoutKey.Value, storedMessage.Value)
		assert.Equal(t, len(messageWithoutKey.Headers), len(storedMessage.Headers))
		assert.True(t, messageWithoutKey.When.Equal(storedMessage.When))
		assert.Equal(t, messageWithoutKey.Where, storedMessage.Where)

		// Cleanup
		defer backend.Delete(ctx, storedMessage.Key)
	})
}

func TestBackend_WriteEmptyMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Failed to create backend")

	err = backend.Connect(ctx)
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer backend.Close()

	// Message with empty value and no headers (minimal message)
	emptyMessage := timebridge.Message{
		Key:     []byte("empty-message-key"),
		Value:   []byte{},                          // Empty value
		Headers: []timebridge.Header{},             // No headers
		When:    time.Now().Add(-15 * time.Minute), // Past time so it will be readable
		Where:   "system-events",
	}

	t.Run("Write empty message", func(t *testing.T) {
		storedMessage, err := backend.Write(ctx, emptyMessage)
		require.NoError(t, err, "Failed to write empty message")
		require.NotNil(t, storedMessage, "Stored message should not be nil")

		// Verify the stored message
		assert.Equal(t, emptyMessage.Key, storedMessage.Message.Key)
		assert.Equal(t, emptyMessage.Value, storedMessage.Value)
		assert.Empty(t, storedMessage.Headers, "Headers should be empty")
		assert.True(t, emptyMessage.When.Equal(storedMessage.When))
		assert.Equal(t, emptyMessage.Where, storedMessage.Where)

		// Cleanup
		defer backend.Delete(ctx, storedMessage.Key)
	})
}

func TestBackend_ReadBatch_FutureMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err)

	err = backend.Connect(ctx)
	require.NoError(t, err)
	defer backend.Close()

	// Create a message scheduled for the future
	futureMessage := timebridge.Message{
		Key:   []byte("future-msg"),
		Value: []byte("future message"),
		When:  time.Now().Add(2 * time.Hour), // Future time
		Where: "future-dest",
	}

	stored, err := backend.Write(ctx, futureMessage)
	require.NoError(t, err)
	defer backend.Delete(ctx, stored.Key)

	t.Run("Future messages not returned", func(t *testing.T) {
		messages, err := backend.ReadBatch(ctx, 10)
		require.NoError(t, err)

		// Future message should not be in results
		for _, msg := range messages {
			assert.NotEqual(t, stored.Key, msg.Key, "Future message should not be returned")
		}
	})
}

func TestBackend_DateTimeHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()
	backend, err := NewBackend(cfg)
	require.NoError(t, err)

	err = backend.Connect(ctx)
	require.NoError(t, err)
	defer backend.Close()

	// Test with various time formats and timezones
	testCases := []struct {
		name string
		when time.Time
	}{
		{
			name: "UTC time",
			when: time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC),
		},
		{
			name: "Local timezone",
			when: time.Date(2024, 12, 25, 10, 0, 0, 0, time.Local),
		},
		{
			name: "Specific timezone",
			when: func() time.Time {
				loc, _ := time.LoadLocation("America/New_York")
				return time.Date(2024, 12, 25, 10, 0, 0, 0, loc)
			}(),
		},
		{
			name: "With nanoseconds",
			when: time.Date(2024, 12, 25, 10, 0, 0, 123456789, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			message := timebridge.Message{
				Key:   []byte("datetime-test"),
				Value: []byte("datetime test message"),
				When:  tc.when.Add(-1 * time.Hour), // Make it past so it's readable
				Where: "datetime-dest",
			}

			stored, err := backend.Write(ctx, message)
			require.NoError(t, err)
			defer backend.Delete(ctx, stored.Key)

			// Read back and verify datetime is preserved
			messages, err := backend.ReadBatch(ctx, 10)
			require.NoError(t, err)

			var foundMessage *timebridge.StoredMessage
			for _, msg := range messages {
				if msg.Key == stored.Key {
					foundMessage = &msg
					break
				}
			}

			require.NotNil(t, foundMessage)

			// MongoDB stores times in UTC, so compare in UTC
			expectedUTC := message.When.UTC()
			actualUTC := foundMessage.When.UTC()

			// Allow for some precision loss (MongoDB stores milliseconds)
			timeDiff := actualUTC.Sub(expectedUTC)
			assert.True(t, timeDiff >= -time.Millisecond && timeDiff <= time.Millisecond,
				"Time difference should be within 1ms, got %v", timeDiff)
		})
	}
}

func TestBackend_WriteError_InvalidConfig(t *testing.T) {
	// Use short timeout context for quick failure
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test with invalid configuration
	cfg := timebridge.MongoDBConfig{
		Database:         "test",
		Collection:       "test",
		Username:         "",
		Password:         "",
		ConnectionString: "mongodb://nonexistent:27017",
		ConnectTimeout:   1, // Short timeout for faster test
		WriteTimeout:     1,
		ReadTimeout:      1,
		DeleteTimeout:    1,
		IndexTimeout:     1,
	}

	backend, err := NewBackend(cfg)
	require.NoError(t, err, "Backend creation should succeed")

	// Connection should fail with invalid config
	err = backend.Connect(ctx)
	assert.Error(t, err, "Connection should fail with invalid config")
}

func TestBackend_ReadBatch_ConcurrentClaiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()

	// Two backend instances simulating two scheduler pods
	backendA, err := NewBackend(cfg)
	require.NoError(t, err)
	require.NoError(t, backendA.Connect(ctx))
	defer backendA.Close()

	backendB, err := NewBackend(cfg)
	require.NoError(t, err)
	require.NoError(t, backendB.Connect(ctx))
	defer backendB.Close()

	// Seed 3 messages with when in the past
	runID := "concurrent-test-" + time.Now().Format("20060102150405.999")
	var seededKeys []string
	for i := 0; i < 3; i++ {
		stored, err := backendA.Write(ctx, timebridge.Message{
			Key:   []byte(runID),
			Value: []byte("test"),
			When:  time.Now().Add(-1 * time.Hour),
			Where: runID,
		})
		require.NoError(t, err)
		seededKeys = append(seededKeys, stored.Key)
	}
	defer func() {
		for _, k := range seededKeys {
			backendA.Delete(ctx, k)
		}
	}()

	time.Sleep(100 * time.Millisecond) // allow index consistency

	// Both backends call ReadBatch concurrently
	var (
		wg             sync.WaitGroup
		batchA, batchB []timebridge.StoredMessage
		errA, errB     error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		batchA, errA = backendA.ReadBatch(ctx, 10)
	}()
	go func() {
		defer wg.Done()
		batchB, errB = backendB.ReadBatch(ctx, 10)
	}()
	wg.Wait() // happens-before: safe to read batchA/batchB/errA/errB after this

	require.NoError(t, errA)
	require.NoError(t, errB)

	// Collect only our seeded keys from each batch
	seededSet := make(map[string]bool, len(seededKeys))
	for _, k := range seededKeys {
		seededSet[k] = true
	}

	keysInA := make(map[string]bool)
	for _, m := range batchA {
		if seededSet[m.Key] {
			keysInA[m.Key] = true
		}
	}
	keysInB := make(map[string]bool)
	for _, m := range batchB {
		if seededSet[m.Key] {
			keysInB[m.Key] = true
		}
	}

	// Each seeded message must appear in exactly one batch
	for _, k := range seededKeys {
		inA, inB := keysInA[k], keysInB[k]
		assert.False(t, inA && inB, "message %s claimed by both instances", k)
	}
	assert.Equal(t, len(seededKeys), len(keysInA)+len(keysInB), "all seeded messages must be claimed exactly once")
}

func TestBackend_ReadBatch_ExpiredLockRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	cfg := getTestConfig()

	cfg.ClaimTTLSeconds = 1 // short TTL so the simulated stale claim is expired
	backend, err := NewBackend(cfg)
	require.NoError(t, err)
	require.NoError(t, backend.Connect(ctx))
	defer backend.Close()

	stored, err := backend.Write(ctx, timebridge.Message{
		Key:   []byte("recovery-test"),
		Value: []byte("test"),
		When:  time.Now().Add(-1 * time.Hour),
		Where: "recovery-dest",
	})
	require.NoError(t, err)
	defer backend.Delete(ctx, stored.Key)

	// Simulate a stale claim: set claimed_until to the past
	_, err = backend.collection.UpdateOne(ctx,
		bson.M{"_id": stored.Key},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "claimed_until", Value: time.Now().Add(-2 * time.Second).UTC()},
			{Key: "claimed_by", Value: "stale-instance"},
		}}},
	)
	require.NoError(t, err)

	// A second backend instance should reclaim the message
	backend2, err := NewBackend(cfg)
	require.NoError(t, err)
	require.NoError(t, backend2.Connect(ctx))
	defer backend2.Close()

	messages, err := backend2.ReadBatch(ctx, 10)
	require.NoError(t, err)

	var found bool
	for _, m := range messages {
		if m.Key == stored.Key {
			found = true
			break
		}
	}
	assert.True(t, found, "message with expired lock should be reclaimed and returned")
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

	t.Log("MongoDB backend correctly implements all required interfaces")
}

package e2e_test

import (
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_NoDuplicates produces 5 messages and asserts each arrives on the destination topic exactly once.
// It runs against a single instance. To validate the multi-instance concurrency fix, run it with
// two timebridge instances active (e.g. docker compose up --scale timebridge=2).
func TestE2E_NoDuplicates(t *testing.T) {
	brokerAddr, ok := os.LookupEnv("E2E_BROKER")
	if !ok {
		t.Skip("E2E_BROKER not set")
	}
	inputTopic, ok := os.LookupEnv("E2E_INPUT_TOPIC")
	if !ok {
		t.Skip("E2E_INPUT_TOPIC not set")
	}
	destTopic, ok := os.LookupEnv("E2E_DEST_TOPIC")
	if !ok {
		t.Skip("E2E_DEST_TOPIC not set")
	}

	const msgCount = 5
	runID := uuid.New().String()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
		"group.id":          "e2e-nodup-" + runID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Subscribe(destTopic, nil))

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
	})
	require.NoError(t, err)
	defer producer.Close()

	deliveryTime := time.Now().Add(3 * time.Second)
	msgKeys := make([]string, msgCount)
	for i := 0; i < msgCount; i++ {
		msgKeys[i] = runID + "-" + uuid.New().String()
		require.NoError(t, producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny},
			Key:            []byte(msgKeys[i]),
			Value:          []byte(`{"test":"no-duplicates"}`),
			Headers: []kafka.Header{
				{Key: "X-Timebridge-When", Value: []byte(deliveryTime.UTC().Format(time.RFC3339))},
				{Key: "X-Timebridge-Where", Value: []byte(destTopic)},
			},
		}, nil))
	}
	producer.Flush(5000)

	// Collect matching messages for up to 30s
	deadline := time.Now().Add(30 * time.Second)
	received := make(map[string]int) // key → count
	for len(received) < msgCount && time.Now().Before(deadline) {
		ev := consumer.Poll(500)
		m, ok := ev.(*kafka.Message)
		if !ok || m.TopicPartition.Error != nil {
			continue
		}
		key := string(m.Key)
		for _, k := range msgKeys {
			if key == k {
				received[key]++
				break
			}
		}
	}

	require.Len(t, received, msgCount, "expected all %d messages to be delivered", msgCount)
	for key, count := range received {
		assert.Equal(t, 1, count, "message %s delivered %d times (expected 1)", key, count)
	}
}

// TestE2E_LoadDistributed produces 100 messages and asserts each arrives exactly once.
// Designed to run with two timebridge instances (--scale timebridge=2) to stress-test
// distributed batch claiming — both instances race to claim and deliver the same pool of messages.
func TestE2E_LoadDistributed(t *testing.T) {
	brokerAddr, ok := os.LookupEnv("E2E_BROKER")
	if !ok {
		t.Skip("E2E_BROKER not set")
	}
	inputTopic, ok := os.LookupEnv("E2E_INPUT_TOPIC")
	if !ok {
		t.Skip("E2E_INPUT_TOPIC not set")
	}
	destTopic, ok := os.LookupEnv("E2E_DEST_TOPIC")
	if !ok {
		t.Skip("E2E_DEST_TOPIC not set")
	}

	const msgCount = 100
	runID := uuid.New().String()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
		"group.id":          "e2e-load-" + runID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Subscribe(destTopic, nil))

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
	})
	require.NoError(t, err)
	defer producer.Close()

	// Index expected keys for O(1) lookup
	deliveryTime := time.Now().Add(3 * time.Second)
	msgKeys := make(map[string]struct{}, msgCount)
	for i := 0; i < msgCount; i++ {
		key := runID + "-" + uuid.New().String()
		msgKeys[key] = struct{}{}
		require.NoError(t, producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(`{"test":"load-distributed"}`),
			Headers: []kafka.Header{
				{Key: "X-Timebridge-When", Value: []byte(deliveryTime.UTC().Format(time.RFC3339))},
				{Key: "X-Timebridge-Where", Value: []byte(destTopic)},
			},
		}, nil))
	}
	producer.Flush(5000)

	// Collect until all messages received or deadline. After all arrive, keep polling
	// for 5 more seconds to catch any duplicates delivered late.
	deadline := time.Now().Add(60 * time.Second)
	received := make(map[string]int, msgCount)
	allReceivedAt := time.Time{}
	for time.Now().Before(deadline) {
		if !allReceivedAt.IsZero() && time.Since(allReceivedAt) > 5*time.Second {
			break
		}
		ev := consumer.Poll(500)
		m, ok := ev.(*kafka.Message)
		if !ok || m.TopicPartition.Error != nil {
			continue
		}
		key := string(m.Key)
		if _, expected := msgKeys[key]; expected {
			received[key]++
			if len(received) == msgCount && allReceivedAt.IsZero() {
				allReceivedAt = time.Now()
				t.Logf("all %d messages received, watching 5s for duplicates...", msgCount)
			}
		}
	}

	require.Len(t, received, msgCount, "expected all %d messages to be delivered", msgCount)
	for key, count := range received {
		assert.Equal(t, 1, count, "message %s delivered %d times (expected 1)", key, count)
	}
}

// TestE2E_ScheduledDelivery is the primary e2e test. It produces a message to the timebridge
// topic scheduled 5 seconds in the future and asserts the message arrives on the destination
// topic after the scheduled time, with timebridge headers stripped.
func TestE2E_ScheduledDelivery(t *testing.T) {
	brokerAddr, ok := os.LookupEnv("E2E_BROKER")
	if !ok {
		t.Skip("E2E_BROKER not set")
	}
	inputTopic, ok := os.LookupEnv("E2E_INPUT_TOPIC")
	if !ok {
		t.Skip("E2E_INPUT_TOPIC not set")
	}
	destTopic, ok := os.LookupEnv("E2E_DEST_TOPIC")
	if !ok {
		t.Skip("E2E_DEST_TOPIC not set")
	}

	runID := uuid.New().String()

	// Subscribe before producing to avoid missing the message.
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
		"group.id":          "e2e-" + runID, // unique per run — no stale offset interference
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Subscribe(destTopic, nil))

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
	})
	require.NoError(t, err)
	defer producer.Close()

	deliveryTime := time.Now().Add(5 * time.Second)
	msgKey := "e2e-test-" + runID
	msgValue := `{"test":"scheduled-delivery"}`

	require.NoError(t, producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny},
		Key:            []byte(msgKey),
		Value:          []byte(msgValue),
		Headers: []kafka.Header{
			{Key: "X-Timebridge-When", Value: []byte(deliveryTime.UTC().Format(time.RFC3339))},
			{Key: "X-Timebridge-Where", Value: []byte(destTopic)},
		},
	}, nil))
	producer.Flush(5000)

	// Poll for the message with a 30s deadline. With SCHEDULER_POLL_INTERVAL_SECONDS=1 and a
	// 5s delay, the message should arrive within ~6s. 30s gives a 5x safety margin.
	deadline := time.Now().Add(30 * time.Second)
	var received *kafka.Message
	for received == nil && time.Now().Before(deadline) {
		ev := consumer.Poll(500)
		switch m := ev.(type) {
		case *kafka.Message:
			if m.TopicPartition.Error == nil && string(m.Key) == msgKey {
				received = m
			}
		case kafka.Error:
			t.Logf("consumer error: %v", m)
		}
	}

	require.NotNilf(t, received, "expected message on %q within 30s", destTopic)
	assert.Equal(t, msgKey, string(received.Key))
	assert.Equal(t, msgValue, string(received.Value))

	for _, h := range received.Headers {
		assert.NotEqual(t, "X-Timebridge-When", h.Key, "X-Timebridge-When header should be stripped by acceptor")
		assert.NotEqual(t, "X-Timebridge-Where", h.Key, "X-Timebridge-Where header should be stripped by acceptor")
	}

	assert.False(t, time.Now().Before(deliveryTime), "message was delivered before its scheduled time")
}

package e2e_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	brokerAddr = mustEnv("E2E_BROKER")
	inputTopic = mustEnv("E2E_INPUT_TOPIC")
	destTopic  = mustEnv("E2E_DEST_TOPIC")
)

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "required env var %s is not set\n", key)
		os.Exit(1)
	}
	return v
}

// TestE2E_ScheduledDelivery is the primary e2e test. It produces a message to the timebridge
// topic scheduled 5 seconds in the future and asserts the message arrives on the destination
// topic after the scheduled time, with timebridge headers stripped.
func TestE2E_ScheduledDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
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
			if m.TopicPartition.Error == nil {
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

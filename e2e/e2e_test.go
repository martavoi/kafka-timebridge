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

// e2eEnv holds required env vars for e2e tests, skipping if any are absent.
func e2eEnv(t *testing.T) (brokerAddr, inputTopic, destTopic string) {
	t.Helper()
	var ok bool
	brokerAddr, ok = os.LookupEnv("E2E_BROKER")
	if !ok {
		t.Skip("E2E_BROKER not set")
	}
	inputTopic, ok = os.LookupEnv("E2E_INPUT_TOPIC")
	if !ok {
		t.Skip("E2E_INPUT_TOPIC not set")
	}
	destTopic, ok = os.LookupEnv("E2E_DEST_TOPIC")
	if !ok {
		t.Skip("E2E_DEST_TOPIC not set")
	}
	return
}

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

// TestE2E_ErrorTopic_InvalidHeaders produces a message without timebridge headers and verifies it
// routes to the error topic — not the destination topic. Requires E2E_ERROR_TOPIC to be set.
func TestE2E_ErrorTopic_InvalidHeaders(t *testing.T) {
	brokerAddr, inputTopic, destTopic := e2eEnv(t)
	errorTopic, ok := os.LookupEnv("E2E_ERROR_TOPIC")
	if !ok {
		t.Skip("E2E_ERROR_TOPIC not set")
	}

	runID := uuid.New().String()
	msgKey := "e2e-error-" + runID

	// Subscribe to both topics before producing
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
		"group.id":          "e2e-error-" + runID,
		"auto.offset.reset": "earliest",
	})
	require.NoError(t, err)
	defer consumer.Close()
	require.NoError(t, consumer.Subscribe(destTopic+","+errorTopic, nil))

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
	})
	require.NoError(t, err)
	defer producer.Close()

	// Produce a message without any timebridge headers — invalid for the acceptor
	require.NoError(t, producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny},
		Key:            []byte(msgKey),
		Value:          []byte(`{"test":"error-routing"}`),
	}, nil))
	producer.Flush(5000)

	// Poll for up to 20s; collect any messages matching our key on either topic
	deadline := time.Now().Add(20 * time.Second)
	var arrivedOnDest, arrivedOnError bool
	for time.Now().Before(deadline) && !(arrivedOnError) {
		ev := consumer.Poll(500)
		m, ok := ev.(*kafka.Message)
		if !ok || m.TopicPartition.Error != nil || string(m.Key) != msgKey {
			continue
		}
		topic := *m.TopicPartition.Topic
		if topic == errorTopic {
			arrivedOnError = true
			t.Logf("message correctly routed to error topic")
		}
		if topic == destTopic {
			arrivedOnDest = true
		}
	}

	assert.True(t, arrivedOnError, "expected message on error topic %q within 20s", errorTopic)
	assert.False(t, arrivedOnDest, "message with invalid headers must not reach destination topic")
}

// TestE2E_ScheduledOrdering produces three messages with different scheduled times in non-chronological
// order and verifies they arrive at the destination topic in when-ascending order.
func TestE2E_ScheduledOrdering(t *testing.T) {
	brokerAddr, inputTopic, destTopic := e2eEnv(t)

	runID := uuid.New().String()
	now := time.Now()

	// Keys encode the expected arrival rank for easy assertion
	type msg struct {
		key  string
		when time.Time
	}
	// Produced out of when-order; should arrive in when-order: second → third → first
	messages := []msg{
		{key: runID + "-first", when: now.Add(10 * time.Second)},
		{key: runID + "-second", when: now.Add(3 * time.Second)},
		{key: runID + "-third", when: now.Add(6 * time.Second)},
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddr,
		"group.id":          "e2e-ordering-" + runID,
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

	expectedKeys := make(map[string]time.Time, len(messages))
	for _, m := range messages {
		expectedKeys[m.key] = m.when
		require.NoError(t, producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &inputTopic, Partition: kafka.PartitionAny},
			Key:            []byte(m.key),
			Value:          []byte(`{"test":"ordering"}`),
			Headers: []kafka.Header{
				{Key: "X-Timebridge-When", Value: []byte(m.when.UTC().Format(time.RFC3339))},
				{Key: "X-Timebridge-Where", Value: []byte(destTopic)},
			},
		}, nil))
	}
	producer.Flush(5000)

	// Collect arrivals in order, tracking actual arrival time
	deadline := time.Now().Add(30 * time.Second)
	type arrival struct {
		key      string
		arrivedAt time.Time
	}
	var arrivals []arrival
	for len(arrivals) < len(messages) && time.Now().Before(deadline) {
		ev := consumer.Poll(500)
		m, ok := ev.(*kafka.Message)
		if !ok || m.TopicPartition.Error != nil {
			continue
		}
		key := string(m.Key)
		if _, expected := expectedKeys[key]; expected {
			arrivals = append(arrivals, arrival{key: key, arrivedAt: time.Now()})
			t.Logf("received %s at %v (scheduled %v)", key, time.Now().Format(time.RFC3339), expectedKeys[key].Format(time.RFC3339))
		}
	}

	require.Len(t, arrivals, len(messages), "expected all %d messages within 30s", len(messages))

	// Verify arrival order matches when-ascending order
	expectedOrder := []string{
		runID + "-second", // when=now+3s — earliest
		runID + "-third",  // when=now+6s
		runID + "-first",  // when=now+10s — latest
	}
	for i, a := range arrivals {
		assert.Equal(t, expectedOrder[i], a.key,
			"arrival position %d: got %s, want %s", i, a.key, expectedOrder[i])
	}
}

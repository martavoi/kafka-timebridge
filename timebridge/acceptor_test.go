package timebridge

import (
	"log/slog"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcceptor_remains_FutureTime(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	futureTime := time.Now().Add(1 * time.Hour)
	result := acceptor.remains(futureTime)

	// Allow small tolerance due to test execution time
	tolerance := 100 * time.Millisecond
	expected := 1 * time.Hour
	assert.InDelta(t, expected.Nanoseconds(), result.Nanoseconds(), float64(tolerance.Nanoseconds()),
		"Should return remaining time for future timestamps")
}

func TestAcceptor_remains_PastTime(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	pastTime := time.Now().Add(-1 * time.Hour)
	result := acceptor.remains(pastTime)

	assert.Equal(t, time.Duration(0), result, "Should return 0 for past timestamps")
}

func TestAcceptor_remains_CurrentTime(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	currentTime := time.Now()
	result := acceptor.remains(currentTime)

	assert.Equal(t, time.Duration(0), result, "Should return 0 for current time")
}

func TestAcceptor_timebridgeHeaders_ValidHeaders(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T15:00:00+03:00")},
		{Key: HeaderTimebridgeWhere, Value: []byte("notifications")},
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "correlation-id", Value: []byte("abc-123")},
	}

	when, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.NoError(t, err)

	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T15:00:00+03:00")
	assert.True(t, when.Equal(expectedTime), "When time should match expected")
	assert.Equal(t, "notifications", where)

	expectedHeaders := []Header{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "correlation-id", Value: []byte("abc-123")},
	}
	assert.Equal(t, expectedHeaders, messageHeaders)
}

func TestAcceptor_timebridgeHeaders_MissingWhenHeader(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhere, Value: []byte("notifications")},
		{Key: "content-type", Value: []byte("application/json")},
	}

	_, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), HeaderTimebridgeWhen)
	assert.Equal(t, "notifications", where)

	expectedHeaders := []Header{
		{Key: "content-type", Value: []byte("application/json")},
	}
	assert.Equal(t, expectedHeaders, messageHeaders)
}

func TestAcceptor_timebridgeHeaders_MissingWhereHeader(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T15:00:00+03:00")},
		{Key: "content-type", Value: []byte("application/json")},
	}

	_, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), HeaderTimebridgeWhere)
	assert.Empty(t, where)

	expectedHeaders := []Header{
		{Key: "content-type", Value: []byte("application/json")},
	}
	assert.Equal(t, expectedHeaders, messageHeaders)
}

func TestAcceptor_timebridgeHeaders_InvalidWhenFormat(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("invalid-date-format")},
		{Key: HeaderTimebridgeWhere, Value: []byte("notifications")},
	}

	_, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), HeaderTimebridgeWhen)
	assert.Equal(t, "notifications", where)
	assert.Empty(t, messageHeaders)
}

func TestAcceptor_timebridgeHeaders_OnlyTimebridgeHeaders(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T15:00:00Z")},
		{Key: HeaderTimebridgeWhere, Value: []byte("alerts")},
	}

	when, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.NoError(t, err)

	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T15:00:00Z")
	assert.True(t, when.Equal(expectedTime), "When time should match expected")
	assert.Equal(t, "alerts", where)
	assert.Empty(t, messageHeaders, "Should have no other headers")
}

func TestAcceptor_timebridgeHeaders_EmptyWhenValue(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("")},
		{Key: HeaderTimebridgeWhere, Value: []byte("notifications")},
	}

	_, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), HeaderTimebridgeWhen)
	assert.Equal(t, "notifications", where)
	assert.Empty(t, messageHeaders)
}

func TestAcceptor_timebridgeHeaders_EmptyWhereValue(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T15:00:00Z")},
		{Key: HeaderTimebridgeWhere, Value: []byte("")},
	}

	_, where, messageHeaders, err := acceptor.timebridgeHeaders(headers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), HeaderTimebridgeWhere)
	assert.Empty(t, where)
	assert.Empty(t, messageHeaders)
}

func TestAcceptor_timebridgeHeaders_UTC_Time(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T12:00:00Z")},
		{Key: HeaderTimebridgeWhere, Value: []byte("test-topic")},
	}

	when, _, _, err := acceptor.timebridgeHeaders(headers)
	require.NoError(t, err)

	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T12:00:00Z")
	assert.True(t, when.UTC().Equal(expectedTime),
		"UTC time should be parsed correctly")
}

func TestAcceptor_timebridgeHeaders_PlusThreeTimezone(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T15:00:00+03:00")},
		{Key: HeaderTimebridgeWhere, Value: []byte("test-topic")},
	}

	when, _, _, err := acceptor.timebridgeHeaders(headers)
	require.NoError(t, err)

	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T12:00:00Z")
	assert.True(t, when.UTC().Equal(expectedTime),
		"Timezone +03:00 should convert correctly to UTC")
}

func TestAcceptor_timebridgeHeaders_MinusFiveTimezone(t *testing.T) {
	acceptor := &Acceptor{
		logger: slog.Default(),
	}

	headers := []kafka.Header{
		{Key: HeaderTimebridgeWhen, Value: []byte("2024-01-01T07:00:00-05:00")},
		{Key: HeaderTimebridgeWhere, Value: []byte("test-topic")},
	}

	when, _, _, err := acceptor.timebridgeHeaders(headers)
	require.NoError(t, err)

	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-01T12:00:00Z")
	assert.True(t, when.UTC().Equal(expectedTime),
		"Timezone -05:00 should convert correctly to UTC")
}

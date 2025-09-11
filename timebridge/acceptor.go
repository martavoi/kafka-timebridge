package timebridge

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	// Timebridge header names
	HeaderTimebridgeWhen  = "X-Timebridge-When"
	HeaderTimebridgeWhere = "X-Timebridge-Where"

	// Error topic headers
	HeaderTimebridgeError = "X-Timebridge-Error"
)

type Acceptor struct {
	logger        *slog.Logger
	consumer      *kafka.Consumer
	backendWriter BackendWriter
	producer      *kafka.Producer
	errorTopic    string
}

func NewAcceptor(logger *slog.Logger, consumer *kafka.Consumer, backendWriter BackendWriter) (*Acceptor, error) {
	return NewAcceptorWithErrorTopic(logger, consumer, backendWriter, nil, "")
}

func NewAcceptorWithErrorTopic(logger *slog.Logger, consumer *kafka.Consumer, backendWriter BackendWriter, producer *kafka.Producer, errorTopic string) (*Acceptor, error) {
	return &Acceptor{
		logger:        logger,
		consumer:      consumer,
		backendWriter: backendWriter,
		producer:      producer,
		errorTopic:    errorTopic,
	}, nil
}

func (a *Acceptor) remains(when time.Time) time.Duration {
	if when.After(time.Now()) {
		return time.Until(when)
	}
	return 0
}

// timebridgeHeaders parses the timebridge (system) headers and returns the when, where, and message headers
func (a *Acceptor) timebridgeHeaders(headers []kafka.Header) (time.Time, string, []Header, error) {
	var whenStr string
	var where string

	var messageHeaders []Header
	for _, header := range headers {
		switch header.Key {
		case HeaderTimebridgeWhen:
			whenStr = string(header.Value)
		case HeaderTimebridgeWhere:
			where = string(header.Value)
		default:
			messageHeaders = append(messageHeaders, Header{
				Key:   header.Key,
				Value: header.Value,
			})
		}
	}

	if whenStr == "" {
		return time.Time{}, where, messageHeaders, fmt.Errorf("%s header is missing", HeaderTimebridgeWhen)
	}

	if where == "" {
		return time.Time{}, where, messageHeaders, fmt.Errorf("%s header is missing", HeaderTimebridgeWhere)
	}

	when, err := time.Parse(time.RFC3339, whenStr)
	if err != nil {
		return time.Time{}, where, messageHeaders, fmt.Errorf("%s header is invalid, RFC3339 format expected", HeaderTimebridgeWhen)
	}

	return when, where, messageHeaders, nil
}

// sendToErrorTopic sends the failed message to the error topic with additional error metadata
func (a *Acceptor) sendToErrorTopic(m *kafka.Message, err error) {

	// Create headers with original message headers plus error metadata
	headers := make([]kafka.Header, len(m.Headers)+1)
	copy(headers, m.Headers)

	// Add error metadata header
	headers = append(headers, kafka.Header{
		Key:   HeaderTimebridgeError,
		Value: []byte(err.Error()),
	})

	errorMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &a.errorTopic, Partition: kafka.PartitionAny},
		Key:            m.Key,
		Value:          m.Value,
		Headers:        headers,
	}

	// Send to error topic (fire and forget for simplicity)
	if err := a.producer.Produce(errorMsg, nil); err != nil {
		a.logger.Error("Failed to send message to error topic",
			"error", err,
			"error_topic", a.errorTopic,
			"message_offset", m.TopicPartition.Offset,
		)
	} else {
		a.logger.Info("Message sent to error topic",
			"error_topic", a.errorTopic,
			"message_offset", m.TopicPartition.Offset,
		)
	}
}

func (a *Acceptor) Run(ctx context.Context) error {
	var run bool = true
	for run {
		select {
		case <-ctx.Done():
			run = false
		default:
			ev := a.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				m := e

				when, where, headers, err := a.timebridgeHeaders(m.Headers)
				if err != nil {
					if a.producer != nil && a.errorTopic != "" {
						a.sendToErrorTopic(m, err)
						a.logger.Warn("Unable to recognize timebridge headers, moving message to the error topic",
							"error", err,
							"message_offset", m.TopicPartition.Offset,
							"error_topic", a.errorTopic,
						)
					} else {
						a.logger.Warn("Unable to recognize timebridge headers, skipping",
							"error", err,
							"message_offset", m.TopicPartition.Offset,
						)
					}

					// Store offset to prevent reprocessing this invalid message
					a.consumer.StoreMessage(m)
					continue
				}

				remains := a.remains(when)
				logger := a.logger.With("when", when, "where", where, "remains", remains)

				writeOp := func() (*StoredMessage, error) {
					storedMessage, err := a.backendWriter.Write(ctx, Message{
						Key:     m.Key,
						Value:   m.Value,
						Headers: headers,
						When:    when,
						Where:   where,
					})

					return storedMessage, err
				}

				notify := func(err error, duration time.Duration) {
					logger.Error("Failed to write message to backend", "error", err, "next_retry", duration)
				}

				backoffStrategy := backoff.NewExponentialBackOff()
				backoffStrategy.InitialInterval = 2 * time.Second
				backoffStrategy.Multiplier = 1.5
				backoffStrategy.MaxInterval = 8 * time.Second

				storedMessage, err := backoff.Retry(ctx, writeOp, backoff.WithBackOff(backoffStrategy), backoff.WithNotify(notify), backoff.WithMaxTries(5))
				if err != nil {
					logger.Error("Failed to write message to backend after retries",
						"error", err,
						"message_offset", m.TopicPartition.Offset,
					)

					// Send to error topic if configured
					if a.producer != nil && a.errorTopic != "" {
						a.sendToErrorTopic(m, err)
					}

					// Store offset to prevent reprocessing this failed message
					a.consumer.StoreMessage(m)
					continue
				}

				logger = logger.With("backend_key", storedMessage.Key)

				a.consumer.StoreMessage(m)
				logger.Info("Received scheduled message")
			case kafka.Error:
				a.logger.Error("Consumer error", "error", e)
			default:
				a.logger.Debug("Ignored event", "event", e)
			}
		}
	}

	return nil
}

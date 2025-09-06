package timebridge

import (
	"context"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	// Timebridge header names
	HeaderTimebridgeWhen  = "X-Timebridge-When"
	HeaderTimebridgeWhere = "X-Timebridge-Where"
)

type Acceptor struct {
	logger      *slog.Logger
	consumer    *kafka.Consumer
	storeWriter BackendWriter
}

func NewAcceptor(logger *slog.Logger, consumer *kafka.Consumer, storeWriter BackendWriter) (*Acceptor, error) {
	return &Acceptor{
		logger:      logger,
		consumer:    consumer,
		storeWriter: storeWriter,
	}, nil
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

				// Extract timebridge headers
				var whenStr string
				var where string

				for _, header := range m.Headers {
					switch header.Key {
					case HeaderTimebridgeWhen:
						whenStr = string(header.Value)
					case HeaderTimebridgeWhere:
						where = string(header.Value)
					}
				}

				logger := a.logger.With("offset", m.TopicPartition.Offset)

				if whenStr == "" {
					logger.Warn("Timebridge when header is missing", "header", HeaderTimebridgeWhen)
					continue
				}

				when, err := time.Parse(time.RFC3339, whenStr)
				if err != nil {
					logger.Warn("Invalid timebridge when header", "header", HeaderTimebridgeWhen, "error", err, "value", whenStr)
					continue
				}

				if where == "" {
					logger.Warn("Timebridge where header is missing", "header", HeaderTimebridgeWhere)
					continue
				}

				logger = logger.With("when", whenStr, "where", where)

				// Filter out timebridge headers and collect remaining headers
				var headers []Header
				for _, header := range m.Headers {
					if header.Key == HeaderTimebridgeWhen || header.Key == HeaderTimebridgeWhere {
						continue // Skip timebridge headers
					}
					headers = append(headers, Header{
						Key:   header.Key,
						Value: header.Value,
					})
				}

				writeOp := func() (*StoredMessage, error) {
					storedMessage, err := a.storeWriter.Write(ctx, Message{
						Key:     m.Key,
						Value:   m.Value,
						Headers: headers,
						When:    when,
						Where:   where,
					})

					return storedMessage, err
				}

				notify := func(err error, duration time.Duration) {
					logger.Error("Failed to write message to store", "error", err, "next_retry", duration)
				}

				backoffStrategy := backoff.NewExponentialBackOff()
				backoffStrategy.InitialInterval = 3 * time.Second
				backoffStrategy.Multiplier = 2
				backoffStrategy.MaxInterval = 1 * time.Hour

				storedMessage, err := backoff.Retry(ctx, writeOp, backoff.WithBackOff(backoffStrategy), backoff.WithNotify(notify))
				if err != nil {
					logger.Error("Failed to write message to store", "error", err)
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

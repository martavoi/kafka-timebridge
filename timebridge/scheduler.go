package timebridge

import (
	"context"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Scheduler struct {
	logger   *slog.Logger
	backend  Backend
	producer *kafka.Producer
	config   SchedulerConfig
}

func NewScheduler(logger *slog.Logger, backend Backend, producer *kafka.Producer, config SchedulerConfig) *Scheduler {
	return &Scheduler{logger: logger, backend: backend, producer: producer, config: config}
}

func (s *Scheduler) Run(ctx context.Context) error {

	var run bool = true
	for run {
		select {
		case <-ctx.Done():
			run = false
		default:
			maxBatchSize := s.config.MaxBatchSize
			readOp := func() ([]StoredMessage, error) {
				batch, err := s.backend.ReadBatch(ctx, maxBatchSize)
				if err != nil {
					return nil, err
				}
				return batch, nil
			}

			notify := func(err error, duration time.Duration) {
				s.logger.Error("Failed to read batch", "error", err, "next_retry", duration)
			}

			backoffStrategy := backoff.NewExponentialBackOff()
			backoffStrategy.InitialInterval = 2 * time.Second
			backoffStrategy.Multiplier = 2
			backoffStrategy.MaxInterval = 1 * time.Hour

			batch, err := backoff.Retry(ctx, readOp, backoff.WithNotify(notify), backoff.WithBackOff(backoffStrategy))
			if err != nil {
				s.logger.Error("Failed to read batch", "error", err)
				continue
			}

			if len(batch) == 0 {
				pollInterval := time.Duration(s.config.PollIntervalSeconds) * time.Second
				s.logger.Debug("No messages to schedule", "next_retry", pollInterval)
				<-time.After(pollInterval)
				continue
			}

			s.logger.Debug("Producing messages...", "count", len(batch))

			// Create delivery channel for this batch
			deliveryChan := make(chan kafka.Event, len(batch))

			var producedCount int = 0
			for _, message := range batch {
				logger := s.logger.With("where", message.Where, "when", message.When, "backend_key", message.Key)
				logger.Debug("Producing message...")
				headers := make([]kafka.Header, len(message.Headers))
				for i, h := range message.Headers {
					headers[i] = kafka.Header{Key: h.Key, Value: h.Value}
				}

				kafkaMsg := &kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &message.Where, Partition: kafka.PartitionAny},
					Key:            message.Message.Key, // Preserve original message key
					Value:          message.Value,
					Headers:        headers,
					Opaque:         message.Key, // Use backend key as opaque data for tracking
				}

				produceOp := func() (any, error) {
					err := s.producer.Produce(kafkaMsg, deliveryChan)
					if err != nil && err.(kafka.Error).Code() != kafka.ErrQueueFull {
						// For non-queue-full errors, don't retry - return a permanent error
						return nil, backoff.Permanent(err)
					}
					return nil, err
				}

				notify := func(err error, duration time.Duration) {
					logger.Warn("Producer queue full, retrying...", "error", err, "next_retry", duration)
				}

				_, err := backoff.Retry(ctx, produceOp, backoff.WithNotify(notify), backoff.WithBackOff(backoffStrategy))
				if err != nil {
					logger.Error("Failed to produce message, skipping...", "error", err)
					continue
				}
				producedCount++
			}

			s.logger.Debug("Messages produced to Kafka, waiting for delivery confirmations...", "produced", producedCount, "failed", len(batch)-producedCount, "total", len(batch))

			// Process delivery reports
			deliveredCount := 0
			failedCount := 0
			processedCount := 0
			for i := 0; i < producedCount; i++ {
				select {
				case e := <-deliveryChan:
					processedCount++
					switch ev := e.(type) {
					case *kafka.Message:
						if ev.TopicPartition.Error != nil {
							failedCount++
							s.logger.Error("Message delivery failed",
								"error", ev.TopicPartition.Error,
								"topic", *ev.TopicPartition.Topic,
								"progress", processedCount, "total", producedCount)
						} else {
							// Extract backend key from opaque data
							backendKey := ev.Opaque.(string)
							s.logger.Debug("Message delivered successfully, deleting from backend...",
								"topic", *ev.TopicPartition.Topic,
								"offset", ev.TopicPartition.Offset,
								"backend_key", backendKey,
								"progress", processedCount, "total", producedCount)

							// Delete successfully delivered message from backend
							if err := s.backend.Delete(ctx, backendKey); err != nil {
								s.logger.Error("Failed to delete message from backend",
									"backend_key", backendKey, "error", err)
								failedCount++
							} else {
								deliveredCount++
								s.logger.Info("Message delivered",
									"backend_key", backendKey)
							}
						}
					case kafka.Error:
						failedCount++
						s.logger.Error("Kafka error during delivery",
							"error", ev,
							"progress", processedCount, "total", producedCount)
					}
				case <-ctx.Done():
					s.logger.Warn("Context cancelled while waiting for delivery reports",
						"processed", processedCount, "total", producedCount)
					return ctx.Err()
				}
			}

			s.logger.Debug("Delivered batch of messages",
				"delivered", deliveredCount,
				"failed", failedCount,
				"total", producedCount)
		}
	}
	return nil
}

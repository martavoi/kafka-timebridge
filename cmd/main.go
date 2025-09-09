package main

import (
	"context"
	"fmt"
	"kafka-timebridge/timebridge"
	"kafka-timebridge/timebridge/couchbase"
	"kafka-timebridge/timebridge/memory"
	"kafka-timebridge/timebridge/mongodb"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/cobra"
)

// version is set at build time via -ldflags "-X main.version=..."
var version = "dev"

var rootCmd = &cobra.Command{
	Use:   "kafka-timebridge",
	Short: "A time-delayed message bridge for Apache Kafka",
	Long: `kafka-timebridge is a service that accepts Kafka messages with future delivery times
and schedules them for re-delivery at the specified time. It supports multiple storage
backends including in-memory and Couchbase for persistence.`,
	Run: runMain,
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("kafka-timebridge version %s\n", version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)

	// Add configuration flags to root command
	rootCmd.PersistentFlags().String("backend", "memory", "Backend type (memory, couchbase, mongodb)")
	rootCmd.PersistentFlags().String("kafka-brokers", "localhost:9092", "Kafka broker addresses")
	rootCmd.PersistentFlags().String("kafka-topic", "timebridge", "Kafka topic name")
	rootCmd.PersistentFlags().String("kafka-group-id", "timebridge", "Kafka consumer group ID")
	rootCmd.PersistentFlags().String("kafka-username", "", "Kafka username")
	rootCmd.PersistentFlags().String("kafka-password", "", "Kafka password")
	rootCmd.PersistentFlags().String("kafka-security-protocol", "PLAINTEXT", "Kafka security protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL)")
	rootCmd.PersistentFlags().String("kafka-sasl-mechanism", "", "Kafka SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level (debug, info, warn, error, fatal)")
	rootCmd.PersistentFlags().String("log-format", "text", "Log format (text, json)")
	rootCmd.PersistentFlags().String("couchbase-bucket", "timebridge", "Couchbase bucket name")
	rootCmd.PersistentFlags().String("couchbase-scope", "timebridge", "Couchbase scope name")
	rootCmd.PersistentFlags().String("couchbase-collection", "messages", "Couchbase collection name")
	rootCmd.PersistentFlags().String("couchbase-username", "timebridge", "Couchbase username")
	rootCmd.PersistentFlags().String("couchbase-password", "", "Couchbase password")
	rootCmd.PersistentFlags().String("couchbase-connection-string", "couchbase://localhost", "Couchbase connection string")
	rootCmd.PersistentFlags().Int("couchbase-upsert-timeout", 2, "Couchbase upsert operation timeout in seconds")
	rootCmd.PersistentFlags().Int("couchbase-query-timeout", 2, "Couchbase query operation timeout in seconds")
	rootCmd.PersistentFlags().Int("couchbase-remove-timeout", 2, "Couchbase remove operation timeout in seconds")
	rootCmd.PersistentFlags().String("mongodb-database", "timebridge", "MongoDB database name")
	rootCmd.PersistentFlags().String("mongodb-collection", "messages", "MongoDB collection name")
	rootCmd.PersistentFlags().String("mongodb-username", "", "MongoDB username")
	rootCmd.PersistentFlags().String("mongodb-password", "", "MongoDB password")
	rootCmd.PersistentFlags().String("mongodb-connection-string", "mongodb://localhost:27017", "MongoDB connection string")
	rootCmd.PersistentFlags().Int("mongodb-connect-timeout", 10, "MongoDB connection timeout in seconds")
	rootCmd.PersistentFlags().Int("mongodb-write-timeout", 5, "MongoDB write operation timeout in seconds")
	rootCmd.PersistentFlags().Int("mongodb-read-timeout", 5, "MongoDB read operation timeout in seconds")
	rootCmd.PersistentFlags().Int("mongodb-delete-timeout", 5, "MongoDB delete operation timeout in seconds")
	rootCmd.PersistentFlags().Int("mongodb-index-timeout", 30, "MongoDB index creation timeout in seconds")
	rootCmd.PersistentFlags().Int("scheduler-max-batch-size", 100, "Maximum number of messages to process in one batch")
	rootCmd.PersistentFlags().Int("scheduler-poll-interval-seconds", 5, "Polling interval in seconds for checking scheduled messages")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runMain(cmd *cobra.Command, args []string) {

	cfg := timebridge.Config{}
	if err := cfg.Load(cmd); err != nil {
		log.Fatal("Failed to load config:", err)
	}

	var level slog.LevelVar
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		log.Fatal("Log level wasn't recognizer:", err)
	}

	// Create logger with configurable format
	var logger *slog.Logger
	handlerOptions := &slog.HandlerOptions{Level: &level}

	switch cfg.LogFormat {
	case "json":
		logger = slog.New(slog.NewJSONHandler(os.Stdout, handlerOptions))
	case "text":
		logger = slog.New(slog.NewTextHandler(os.Stdout, handlerOptions))
	default:
		logger = slog.New(slog.NewTextHandler(os.Stdout, handlerOptions))
	}

	logger.Debug("Config loaded",
		"backend", cfg.Backend,
		"log_level", cfg.LogLevel,
		"log_format", cfg.LogFormat,
		"kafka_brokers", cfg.Kafka.Brokers,
		"kafka_topic", cfg.Kafka.Topic,
		"kafka_group_id", cfg.Kafka.GroupId,
		"kafka_username", cfg.Kafka.Username,
		"kafka_password", cfg.Kafka.Password,
		"kafka_security_protocol", cfg.Kafka.SecurityProtocol,
		"kafka_sasl_mechanism", cfg.Kafka.SaslMechanism,
	)

	// Only log Couchbase config if it's being used
	if cfg.Backend == "couchbase" {
		logger.Debug("Couchbase config",
			"couchbase_bucket", cfg.Couchbase.Bucket,
			"couchbase_scope", cfg.Couchbase.Scope,
			"couchbase_collection", cfg.Couchbase.Collection,
			"couchbase_username", cfg.Couchbase.Username,
			"couchbase_password", cfg.Couchbase.Password,
			"couchbase_connection_string", cfg.Couchbase.ConnectionString,
		)
	}

	// Only log MongoDB config if it's being used
	if cfg.Backend == "mongodb" {
		logger.Debug("MongoDB config",
			"mongodb_database", cfg.MongoDB.Database,
			"mongodb_collection", cfg.MongoDB.Collection,
			"mongodb_username", cfg.MongoDB.Username,
			"mongodb_password", cfg.MongoDB.Password,
			"mongodb_connection_string", cfg.MongoDB.ConnectionString,
		)
	}

	// Create backend based on configuration
	var backend timebridge.Backend
	switch cfg.Backend {
	case timebridge.BackendMemory:
		logger.Info("Using memory backend")
		backend = memory.NewBackend()
	case timebridge.BackendCouchbase:
		logger.Info("Using Couchbase backend")
		cbBackend, err := couchbase.NewBackend(cfg.Couchbase)
		if err != nil {
			logger.Error("Failed to create Couchbase backend", "error", err)
			return
		}

		logger.Debug("Connecting to Couchbase...")
		err = cbBackend.Connect()
		if err != nil {
			logger.Error("Failed to connect to Couchbase", "error", err)
			return
		}

		backend = cbBackend
	case timebridge.BackendMongoDB:
		logger.Info("Using MongoDB backend")
		mongoBackend, err := mongodb.NewBackend(cfg.MongoDB)
		if err != nil {
			logger.Error("Failed to create MongoDB backend", "error", err)
			return
		}

		logger.Debug("Connecting to MongoDB...")
		err = mongoBackend.Connect()
		if err != nil {
			logger.Error("Failed to connect to MongoDB", "error", err)
			return
		}

		backend = mongoBackend
	default:
		logger.Error("Unknown backend type", "backend", cfg.Backend,
			"supported", []string{timebridge.BackendMemory, timebridge.BackendCouchbase, timebridge.BackendMongoDB})
		return
	}
	defer backend.Close()

	// Create Kafka consumer
	logger.Debug("Creating Kafka consumer...", "brokers", cfg.Kafka.Brokers, "group_id", cfg.Kafka.GroupId)

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": cfg.Kafka.Brokers,
		"group.id":          cfg.Kafka.GroupId,
		"auto.offset.reset": "earliest",
	}

	// Add authentication if credentials are provided
	if cfg.Kafka.Username != "" && cfg.Kafka.Password.String() != "" {
		consumerConfig.SetKey("security.protocol", cfg.Kafka.SecurityProtocol)
		consumerConfig.SetKey("sasl.mechanism", cfg.Kafka.SaslMechanism)
		consumerConfig.SetKey("sasl.username", cfg.Kafka.Username)
		consumerConfig.SetKey("sasl.password", cfg.Kafka.Password.String())
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		logger.Error("Failed to create Kafka consumer:", "error", err)
		return
	}
	defer consumer.Close()

	// Subscribe to topic
	logger.Debug("Subscribing to topic...", "topic", cfg.Kafka.Topic)
	err = consumer.Subscribe(cfg.Kafka.Topic, nil)
	if err != nil {
		logger.Error("Failed to subscribe to topic", "error", err, "topic", cfg.Kafka.Topic)
		return
	}
	logger.Debug("Successfully subscribed to topic", "topic", cfg.Kafka.Topic)

	// Create acceptor with consumer and backend
	acceptor, err := timebridge.NewAcceptor(logger, consumer, backend)
	if err != nil {
		log.Fatal("Failed to create acceptor:", err)
	}

	acceptorCtx, acceptorCancel := context.WithCancel(context.Background())
	defer acceptorCancel()

	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers": cfg.Kafka.Brokers,
	}
	if cfg.Kafka.Username != "" && cfg.Kafka.Password.String() != "" {
		producerConfig.SetKey("security.protocol", cfg.Kafka.SecurityProtocol)
		producerConfig.SetKey("sasl.mechanism", cfg.Kafka.SaslMechanism)
		producerConfig.SetKey("sasl.username", cfg.Kafka.Username)
		producerConfig.SetKey("sasl.password", cfg.Kafka.Password.String())
	}
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		logger.Error("Failed to create Kafka producer", "error", err)
		return
	}
	defer producer.Close()

	scheduler := timebridge.NewScheduler(logger, backend, producer, cfg.Scheduler)

	schedulerCtx, schedulerCancel := context.WithCancel(context.Background())
	defer schedulerCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run acceptor in goroutine
	acceptorDone := make(chan error, 1)
	go func() {
		acceptorDone <- acceptor.Run(acceptorCtx)
	}()

	// Run scheduler in goroutine
	schedulerDone := make(chan error, 1)
	go func() {
		schedulerDone <- scheduler.Run(schedulerCtx)
	}()

	// Wait for either completion or signal
	select {
	case <-sigChan:
		logger.Info("Received shutdown signal, stopping gracefully...")
		acceptorCancel()  // Cancel context to stop acceptor
		schedulerCancel() // Cancel context to stop scheduler

		// Wait for both acceptor and scheduler to finish cleanup
		logger.Debug("Waiting for acceptor to shutdown...")
		select {
		case err := <-acceptorDone:
			if err != nil && err != context.Canceled {
				logger.Error("Timebridge acceptor stopped with error", "error", err)
			} else {
				logger.Debug("Timebridge acceptor stopped gracefully")
			}
		case <-time.After(5 * time.Second):
			logger.Warn("Acceptor shutdown timed out")
		}

		logger.Debug("Waiting for scheduler to shutdown...")
		select {
		case err := <-schedulerDone:
			if err != nil && err != context.Canceled {
				logger.Error("Timebridge scheduler stopped with error", "error", err)
			} else {
				logger.Debug("Timebridge scheduler stopped gracefully")
			}
		case <-time.After(5 * time.Second):
			logger.Warn("Scheduler shutdown timed out")
		}

	case err := <-acceptorDone:
		logger.Error("Timebridge acceptor failed, shutting down system", "error", err)
		schedulerCancel() // Stop scheduler if acceptor fails

		// Wait for scheduler to stop
		select {
		case err := <-schedulerDone:
			if err != nil && err != context.Canceled {
				logger.Error("Timebridge scheduler stopped with error during shutdown", "error", err)
			}
		case <-time.After(5 * time.Second):
			logger.Warn("Scheduler shutdown timed out during acceptor failure")
		}
		return

	case err := <-schedulerDone:
		logger.Error("Timebridge scheduler failed, shutting down system", "error", err)
		acceptorCancel() // Stop acceptor if scheduler fails

		// Wait for acceptor to stop
		select {
		case err := <-acceptorDone:
			if err != nil && err != context.Canceled {
				logger.Error("Timebridge acceptor stopped with error during shutdown", "error", err)
			}
		case <-time.After(5 * time.Second):
			logger.Warn("Acceptor shutdown timed out during scheduler failure")
		}
		return
	}

	logger.Info("Timebridge stopped successfully")
}

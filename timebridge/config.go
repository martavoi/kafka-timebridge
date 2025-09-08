package timebridge

import (
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	Backend   string          `mapstructure:"backend"`
	Kafka     KafkaConfig     `mapstructure:"kafka,"`
	Couchbase CouchbaseConfig `mapstructure:"couchbase,"`
	Scheduler SchedulerConfig `mapstructure:"scheduler,"`
	LogLevel  string          `mapstructure:"log_level"`
	LogFormat string          `mapstructure:"log_format"`
}

func (c *Config) Load(cmd *cobra.Command) error {
	// Bind Cobra flags to viper keys (if command provided)
	if cmd != nil {
		viper.BindPFlag("backend", cmd.Flags().Lookup("backend"))
		viper.BindPFlag("kafka.brokers", cmd.Flags().Lookup("kafka-brokers"))
		viper.BindPFlag("kafka.topic", cmd.Flags().Lookup("kafka-topic"))
		viper.BindPFlag("kafka.group_id", cmd.Flags().Lookup("kafka-group-id"))
		viper.BindPFlag("kafka.username", cmd.Flags().Lookup("kafka-username"))
		viper.BindPFlag("kafka.password", cmd.Flags().Lookup("kafka-password"))
		viper.BindPFlag("kafka.securityprotocol", cmd.Flags().Lookup("kafka-security-protocol"))
		viper.BindPFlag("kafka.saslmechanism", cmd.Flags().Lookup("kafka-sasl-mechanism"))
		viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
		viper.BindPFlag("log_format", cmd.Flags().Lookup("log-format"))
		viper.BindPFlag("couchbase.bucket", cmd.Flags().Lookup("couchbase-bucket"))
		viper.BindPFlag("couchbase.scope", cmd.Flags().Lookup("couchbase-scope"))
		viper.BindPFlag("couchbase.collection", cmd.Flags().Lookup("couchbase-collection"))
		viper.BindPFlag("couchbase.username", cmd.Flags().Lookup("couchbase-username"))
		viper.BindPFlag("couchbase.password", cmd.Flags().Lookup("couchbase-password"))
		viper.BindPFlag("couchbase.connection_string", cmd.Flags().Lookup("couchbase-connection-string"))
		viper.BindPFlag("couchbase.upsert_timeout", cmd.Flags().Lookup("couchbase-upsert-timeout"))
		viper.BindPFlag("couchbase.query_timeout", cmd.Flags().Lookup("couchbase-query-timeout"))
		viper.BindPFlag("couchbase.remove_timeout", cmd.Flags().Lookup("couchbase-remove-timeout"))
		viper.BindPFlag("scheduler.max_batch_size", cmd.Flags().Lookup("scheduler-max-batch-size"))
		viper.BindPFlag("scheduler.poll_interval_seconds", cmd.Flags().Lookup("scheduler-poll-interval-seconds"))
	}

	// Bind environment variables
	viper.BindEnv("backend", "BACKEND")
	viper.BindEnv("kafka.brokers", "KAFKA_BROKERS")
	viper.BindEnv("kafka.topic", "KAFKA_TOPIC")
	viper.BindEnv("kafka.groupid", "KAFKA_GROUP_ID")
	viper.BindEnv("kafka.username", "KAFKA_USERNAME")
	viper.BindEnv("kafka.password", "KAFKA_PASSWORD")
	viper.BindEnv("kafka.securityprotocol", "KAFKA_SECURITY_PROTOCOL")
	viper.BindEnv("kafka.saslmechanism", "KAFKA_SASL_MECHANISM")
	viper.BindEnv("log_level", "LOG_LEVEL")
	viper.BindEnv("log_format", "LOG_FORMAT")
	viper.BindEnv("couchbase.bucket", "COUCHBASE_BUCKET")
	viper.BindEnv("couchbase.scope", "COUCHBASE_SCOPE")
	viper.BindEnv("couchbase.collection", "COUCHBASE_COLLECTION")
	viper.BindEnv("couchbase.username", "COUCHBASE_USERNAME")
	viper.BindEnv("couchbase.password", "COUCHBASE_PASSWORD")
	viper.BindEnv("couchbase.connectionString", "COUCHBASE_CONNECTION_STRING")
	viper.BindEnv("couchbase.upsert_timeout", "COUCHBASE_UPSERT_TIMEOUT")
	viper.BindEnv("couchbase.query_timeout", "COUCHBASE_QUERY_TIMEOUT")
	viper.BindEnv("couchbase.remove_timeout", "COUCHBASE_REMOVE_TIMEOUT")
	viper.BindEnv("scheduler.max_batch_size", "SCHEDULER_MAX_BATCH_SIZE")
	viper.BindEnv("scheduler.poll_interval_seconds", "SCHEDULER_POLL_INTERVAL_SECONDS")

	// Set defaults
	viper.SetDefault("backend", BackendMemory)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("log_format", "text")
	viper.SetDefault("kafka.brokers", "localhost:9092")
	viper.SetDefault("kafka.topic", "timebridge")
	viper.SetDefault("kafka.groupid", "timebridge")
	viper.SetDefault("kafka.securityprotocol", "PLAINTEXT")
	viper.SetDefault("kafka.saslmechanism", "")
	// Couchbase defaults - only used when backend is "couchbase"
	viper.SetDefault("couchbase.bucket", "timebridge")
	viper.SetDefault("couchbase.scope", "timebridge")
	viper.SetDefault("couchbase.collection", "messages")
	viper.SetDefault("couchbase.username", "timebridge")
	viper.SetDefault("couchbase.connectionString", "couchbase://localhost")
	viper.SetDefault("couchbase.upsert_timeout", 2)
	viper.SetDefault("couchbase.query_timeout", 2)
	viper.SetDefault("couchbase.remove_timeout", 2)
	// Scheduler defaults
	viper.SetDefault("scheduler.max_batch_size", 100)
	viper.SetDefault("scheduler.poll_interval_seconds", 5)

	return viper.Unmarshal(c)
}

// SecretString masks password/sensitive values in logs
type SecretString string

func (s SecretString) LogValue() slog.Value {
	if s == "" {
		return slog.StringValue("")
	}
	return slog.StringValue("***")
}

func (s SecretString) String() string {
	return string(s)
}

type KafkaConfig struct {
	Brokers          string `validate:"required,comma_separated_list"`
	Topic            string `validate:"required"`
	GroupId          string `validate:"required"`
	Username         string
	Password         SecretString
	SecurityProtocol string
	SaslMechanism    string
}

type CouchbaseConfig struct {
	Bucket           string       `validate:"omitempty"`
	Scope            string       `validate:"omitempty"`
	Collection       string       `validate:"omitempty"`
	Username         string       `validate:"omitempty"`
	Password         SecretString `validate:"omitempty"`
	ConnectionString string       `validate:"omitempty"`
	UpsertTimeout    int          `mapstructure:"upsert_timeout"`
	QueryTimeout     int          `mapstructure:"query_timeout"`
	RemoveTimeout    int          `mapstructure:"remove_timeout"`
}

type SchedulerConfig struct {
	MaxBatchSize        int `mapstructure:"max_batch_size"`
	PollIntervalSeconds int `mapstructure:"poll_interval_seconds"`
}

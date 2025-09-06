package timebridge

import (
	"log/slog"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	Backend   string          `mapstructure:"backend"`
	Kafka     KafkaConfig     `mapstructure:"kafka,"`
	Couchbase CouchbaseConfig `mapstructure:"couchbase,"`
	LogLevel  string          `mapstructure:"log_level"`
	LogFormat string          `mapstructure:"log_format"`
}

func (c *Config) Load() error {
	// Define CLI flags
	pflag.String("backend", "", "Backend type (memory, couchbase)")
	pflag.String("kafka-brokers", "", "Kafka broker addresses")
	pflag.String("kafka-topic", "", "Kafka topic name")
	pflag.String("kafka-group-id", "", "Kafka consumer group ID")
	pflag.String("kafka-username", "", "Kafka username")
	pflag.String("kafka-password", "", "Kafka password")
	pflag.String("kafka-security-protocol", "", "Kafka security protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL)")
	pflag.String("kafka-sasl-mechanism", "", "Kafka SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	pflag.String("log-level", "", "Log level (debug, info, warn, error, fatal)")
	pflag.String("log-format", "", "Log format (text, json)")
	pflag.String("couchbase-bucket", "", "Couchbase bucket name")
	pflag.String("couchbase-scope", "", "Couchbase scope name")
	pflag.String("couchbase-collection", "", "Couchbase collection name")
	pflag.String("couchbase-username", "", "Couchbase username")
	pflag.String("couchbase-password", "", "Couchbase password")
	pflag.String("couchbase-connection-string", "", "Couchbase connection string")
	pflag.Parse()

	// Bind CLI flags to viper keys
	viper.BindPFlag("backend", pflag.Lookup("backend"))
	viper.BindPFlag("kafka.brokers", pflag.Lookup("kafka-brokers"))
	viper.BindPFlag("kafka.topic", pflag.Lookup("kafka-topic"))
	viper.BindPFlag("kafka.group_id", pflag.Lookup("kafka-group-id"))
	viper.BindPFlag("kafka.username", pflag.Lookup("kafka-username"))
	viper.BindPFlag("kafka.password", pflag.Lookup("kafka-password"))
	viper.BindPFlag("kafka.securityprotocol", pflag.Lookup("kafka-security-protocol"))
	viper.BindPFlag("kafka.saslmechanism", pflag.Lookup("kafka-sasl-mechanism"))
	viper.BindPFlag("log_level", pflag.Lookup("log-level"))
	viper.BindPFlag("log_format", pflag.Lookup("log-format"))
	viper.BindPFlag("couchbase.bucket", pflag.Lookup("couchbase-bucket"))
	viper.BindPFlag("couchbase.scope", pflag.Lookup("couchbase-scope"))
	viper.BindPFlag("couchbase.collection", pflag.Lookup("couchbase-collection"))
	viper.BindPFlag("couchbase.username", pflag.Lookup("couchbase-username"))
	viper.BindPFlag("couchbase.password", pflag.Lookup("couchbase-password"))
	viper.BindPFlag("couchbase.connection_string", pflag.Lookup("couchbase-connection-string"))

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

	// Set defaults
	viper.SetDefault("backend", BackendMemory)
	viper.SetDefault("log_level", "debug")
	viper.SetDefault("log_format", "text")
	viper.SetDefault("kafka.brokers", "localhost:9092")
	viper.SetDefault("kafka.topic", "timebridge")
	viper.SetDefault("kafka.groupid", "timebridge")
	viper.SetDefault("kafka.securityprotocol", "SASL_PLAINTEXT")
	viper.SetDefault("kafka.saslmechanism", "PLAIN")
	// Couchbase defaults - only used when backend is "couchbase"
	viper.SetDefault("couchbase.bucket", "timebridge")
	viper.SetDefault("couchbase.scope", "timebridge")
	viper.SetDefault("couchbase.collection", "messages")
	viper.SetDefault("couchbase.username", "timebridge")
	viper.SetDefault("couchbase.connectionString", "couchbase://localhost")

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
}

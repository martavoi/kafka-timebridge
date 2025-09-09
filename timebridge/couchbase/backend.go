package couchbase

import (
	"context"
	"fmt"
	"kafka-timebridge/timebridge"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

type MessageHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type MessageDocument struct {
	Key     []byte          `json:"key"`
	Value   []byte          `json:"value"`
	Headers []MessageHeader `json:"headers"`
	When    int64           `json:"when"` // Unix timestamp in seconds for reliable comparison
	Where   string          `json:"where"`
}

type Backend struct {
	cfg     timebridge.CouchbaseConfig
	cluster *gocb.Cluster
}

func NewBackend(cfg timebridge.CouchbaseConfig) (*Backend, error) {
	return &Backend{cfg: cfg}, nil
}

func (s *Backend) Connect() error {
	cluster, err := gocb.Connect(s.cfg.ConnectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: s.cfg.Username,
			Password: s.cfg.Password.String(),
		},
	})
	if err != nil {
		return err
	}

	s.cluster = cluster

	// Create index on 'when' field for efficient queries (if enabled)
	if s.cfg.AutoCreateIndex {
		// Create index with IF NOT EXISTS to avoid errors for existing indexes
		// Index naming convention: letters, digits, underscore, hash - must start with letter
		indexName := "timebridge_when_idx"
		indexQuery := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON `%s`.`%s`.`%s`(`when`)",
			indexName, s.cfg.Bucket, s.cfg.Scope, s.cfg.Collection)

		_, err = s.cluster.Query(indexQuery, &gocb.QueryOptions{
			Timeout: time.Duration(s.cfg.IndexTimeout) * time.Second,
		})
		if err != nil {
			// Index creation failed - this could indicate connection issues
			// or insufficient permissions, so we should return the error
			return err
		}
	}

	return nil
}

func (s *Backend) Close() error {
	return s.cluster.Close(&gocb.ClusterCloseOptions{})
}

func (s *Backend) Write(ctx context.Context, m timebridge.Message) (*timebridge.StoredMessage, error) {
	bucket := s.cluster.Bucket(s.cfg.Bucket)
	scope := bucket.Scope(s.cfg.Scope)
	collection := scope.Collection(s.cfg.Collection)

	headers := make([]MessageHeader, len(m.Headers))
	for i, h := range m.Headers {
		headers[i] = MessageHeader(h)
	}
	doc := MessageDocument{
		Key:     m.Key,
		Value:   m.Value,
		Headers: headers,
		When:    m.When.Unix(), // Store as Unix timestamp for reliable comparison
		Where:   m.Where,
	}

	key := uuid.New().String()

	_, err := collection.Upsert(key, doc, &gocb.UpsertOptions{
		Timeout: time.Duration(s.cfg.UpsertTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &timebridge.StoredMessage{
		Message: m,
		Key:     key,
	}, nil
}

func (s *Backend) ReadBatch(ctx context.Context, limit int) ([]timebridge.StoredMessage, error) {
	bucket := s.cluster.Bucket(s.cfg.Bucket)
	scope := bucket.Scope(s.cfg.Scope)
	collection := scope.Collection(s.cfg.Collection)

	// Query only messages that are ready to be delivered (when <= now), ordered by when ASC (oldest first)
	// Use Unix timestamp comparison for reliable results across timezones
	nowUnix := time.Now().Unix()
	// Use full keyspace path: bucket.scope.collection
	keyspace := fmt.Sprintf("`%s`.`%s`.`%s`", bucket.Name(), scope.Name(), collection.Name())
	query := fmt.Sprintf("SELECT META().id, `key`, `value`, `headers`, `when`, `where` FROM %s WHERE `when` <= %d ORDER BY `when` ASC LIMIT %d", keyspace, nowUnix, limit)

	rows, err := s.cluster.Query(query, &gocb.QueryOptions{
		Timeout: time.Duration(s.cfg.QueryTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	docs := make([]timebridge.StoredMessage, 0)
	for rows.Next() {
		// Structure to capture both document ID and content
		// With SELECT META().id, *, the document fields are flattened at the top level
		var QueryResultRow struct {
			Id      string          `json:"id"`
			Key     []byte          `json:"key"`
			Value   []byte          `json:"value"`
			Headers []MessageHeader `json:"headers"`
			When    int64           `json:"when"` // Unix timestamp
			Where   string          `json:"where"`
		}

		err := rows.Row(&QueryResultRow)
		if err != nil {
			return nil, err
		}

		headers := make([]timebridge.Header, len(QueryResultRow.Headers))
		for i, h := range QueryResultRow.Headers {
			headers[i] = timebridge.Header(h)
		}

		docs = append(docs, timebridge.StoredMessage{
			Message: timebridge.Message{
				Key:     QueryResultRow.Key,
				Value:   QueryResultRow.Value,
				Headers: headers,
				When:    time.Unix(QueryResultRow.When, 0).UTC(), // Convert Unix timestamp back to time.Time in UTC
				Where:   QueryResultRow.Where,
			},
			Key: QueryResultRow.Id,
		})
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return docs, nil
}

func (s *Backend) Delete(ctx context.Context, key string) error {
	bucket := s.cluster.Bucket(s.cfg.Bucket)
	scope := bucket.Scope(s.cfg.Scope)
	collection := scope.Collection(s.cfg.Collection)

	_, err := collection.Remove(key, &gocb.RemoveOptions{
		Timeout: time.Duration(s.cfg.RemoveTimeout) * time.Second,
	})
	return err
}

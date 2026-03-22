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
	Key          []byte          `json:"key"`
	Value        []byte          `json:"value"`
	Headers      []MessageHeader `json:"headers"`
	When         int64           `json:"when"`          // Unix timestamp in seconds for reliable comparison
	Where        string          `json:"where"`
	ClaimedUntil int64           `json:"claimed_until"` // Unix timestamp; 0 = unclaimed
	ClaimedBy    string          `json:"claimed_by"`    // batch claim UUID, "" when unclaimed
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

	// Create indexes for efficient queries (if enabled)
	if s.cfg.AutoCreateIndex {
		keyspace := fmt.Sprintf("`%s`.`%s`.`%s`", s.cfg.Bucket, s.cfg.Scope, s.cfg.Collection)

		// Hot-path: find unclaimed/re-claimable messages ready for delivery
		idx := fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS timebridge_when_claimed_idx ON %s(`when`, claimed_until)",
			keyspace)
		_, err = s.cluster.Query(idx, &gocb.QueryOptions{
			Timeout: time.Duration(s.cfg.IndexTimeout) * time.Second,
		})
		if err != nil {
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
		// ClaimedUntil and ClaimedBy are zero values — document is unclaimed by default
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
	keyspace := fmt.Sprintf("`%s`.`%s`.`%s`", bucket.Name(), scope.Name(), collection.Name())

	nowUnix := time.Now().Unix()
	claimedUntil := nowUnix + int64(s.cfg.ClaimTTLSeconds)
	claimToken := uuid.New().String()
	queryTimeout := time.Duration(s.cfg.QueryTimeout) * time.Second

	// Step 1: Find candidate document IDs — ready and unclaimed/expired.
	// claimed_until <= now covers both unclaimed (0) and expired claims.
	candidateRows, err := s.cluster.Query(
		fmt.Sprintf(
			"SELECT META().id FROM %s "+
				"WHERE `when` <= $now AND claimed_until <= $now "+
				"ORDER BY `when` ASC LIMIT $limit",
			keyspace),
		&gocb.QueryOptions{
			Timeout: queryTimeout,
			NamedParameters: map[string]any{
				"now":   nowUnix,
				"limit": limit,
			},
		})
	if err != nil {
		return nil, err
	}
	defer candidateRows.Close()

	var candidateIDs []string
	for candidateRows.Next() {
		var row struct {
			ID string `json:"id"`
		}
		if err := candidateRows.Row(&row); err != nil {
			return nil, err
		}
		candidateIDs = append(candidateIDs, row.ID)
	}
	if err := candidateRows.Err(); err != nil {
		return nil, err
	}
	if len(candidateIDs) == 0 {
		return nil, nil
	}

	// Step 2: Atomically claim — USE KEYS targets docs directly by ID;
	// re-checking claimed_until <= now ensures concurrent instances claim disjoint subsets.
	_, err = s.cluster.Query(
		fmt.Sprintf(
			"UPDATE %s USE KEYS $ids "+
				"SET claimed_until = $claimedUntil, claimed_by = $claimToken "+
				"WHERE claimed_until <= $now",
			keyspace),
		&gocb.QueryOptions{
			Timeout: queryTimeout,
			NamedParameters: map[string]any{
				"ids":          candidateIDs,
				"claimedUntil": claimedUntil,
				"claimToken":   claimToken,
				"now":          nowUnix,
			},
		})
	if err != nil {
		return nil, err
	}

	// Step 3: Fetch documents claimed by this instance.
	// USE KEYS limits to the candidate set; WHERE filters to only what we won.
	rows, err := s.cluster.Query(
		fmt.Sprintf(
			"SELECT META().id, `key`, `value`, `headers`, `when`, `where` FROM %s "+
				"USE KEYS $ids WHERE claimed_by = $claimToken ORDER BY `when` ASC",
			keyspace),
		&gocb.QueryOptions{
			Timeout: queryTimeout,
			NamedParameters: map[string]any{
				"ids":        candidateIDs,
				"claimToken": claimToken,
			},
		})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	docs := make([]timebridge.StoredMessage, 0)
	for rows.Next() {
		var row struct {
			Id      string          `json:"id"`
			Key     []byte          `json:"key"`
			Value   []byte          `json:"value"`
			Headers []MessageHeader `json:"headers"`
			When    int64           `json:"when"`
			Where   string          `json:"where"`
		}
		if err := rows.Row(&row); err != nil {
			return nil, err
		}

		headers := make([]timebridge.Header, len(row.Headers))
		for i, h := range row.Headers {
			headers[i] = timebridge.Header(h)
		}
		docs = append(docs, timebridge.StoredMessage{
			Message: timebridge.Message{
				Key:     row.Key,
				Value:   row.Value,
				Headers: headers,
				When:    time.Unix(row.When, 0).UTC(),
				Where:   row.Where,
			},
			Key: row.Id,
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

package mongodb

import (
	"context"
	"kafka-timebridge/timebridge"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MessageHeader struct {
	Key   string `bson:"key"`
	Value []byte `bson:"value"`
}

type MessageDocument struct {
	ID      string          `bson:"_id,omitempty"`
	Key     []byte          `bson:"key"`
	Value   []byte          `bson:"value"`
	Headers []MessageHeader `bson:"headers"`
	When    time.Time       `bson:"when"`
	Where   string          `bson:"where"`
}

type Backend struct {
	cfg        timebridge.MongoDBConfig
	client     *mongo.Client
	collection *mongo.Collection
}

func NewBackend(cfg timebridge.MongoDBConfig) (*Backend, error) {
	return &Backend{cfg: cfg}, nil
}

func (b *Backend) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.ConnectTimeout)*time.Second)
	defer cancel()

	// Create client options
	clientOptions := options.Client().ApplyURI(b.cfg.ConnectionString)

	// Add authentication if credentials are provided
	if b.cfg.Username != "" && b.cfg.Password.String() != "" {
		credential := options.Credential{
			Username: b.cfg.Username,
			Password: b.cfg.Password.String(),
		}
		clientOptions.SetAuth(credential)
	}

	// Create client and connect
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return err
	}

	// Ping to verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		client.Disconnect(ctx)
		return err
	}

	b.client = client
	b.collection = client.Database(b.cfg.Database).Collection(b.cfg.Collection)

	// Create index on 'when' field for efficient queries
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: "when", Value: 1}}, // Ascending index on 'when' field
	}

	indexCtx, indexCancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.IndexTimeout)*time.Second)
	defer indexCancel()

	_, err = b.collection.Indexes().CreateOne(indexCtx, indexModel)
	if err != nil {
		// Don't fail if index already exists
		// Just log and continue
	}

	return nil
}

func (b *Backend) Close() error {
	if b.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return b.client.Disconnect(ctx)
	}
	return nil
}

func (b *Backend) Write(ctx context.Context, m timebridge.Message) (*timebridge.StoredMessage, error) {
	headers := make([]MessageHeader, len(m.Headers))
	for i, h := range m.Headers {
		headers[i] = MessageHeader{
			Key:   h.Key,
			Value: h.Value,
		}
	}

	doc := MessageDocument{
		ID:      uuid.New().String(),
		Key:     m.Key,
		Value:   m.Value,
		Headers: headers,
		When:    m.When.UTC(), // Store as UTC for consistency
		Where:   m.Where,
	}

	writeCtx, cancel := context.WithTimeout(ctx, time.Duration(b.cfg.WriteTimeout)*time.Second)
	defer cancel()

	_, err := b.collection.InsertOne(writeCtx, doc)
	if err != nil {
		return nil, err
	}

	return &timebridge.StoredMessage{
		Message: m,
		Key:     doc.ID,
	}, nil
}

func (b *Backend) ReadBatch(ctx context.Context, limit int) ([]timebridge.StoredMessage, error) {
	readCtx, cancel := context.WithTimeout(ctx, time.Duration(b.cfg.ReadTimeout)*time.Second)
	defer cancel()

	// Query only messages that are ready to be delivered (when <= now), ordered by when ASC (oldest first)
	filter := bson.M{"when": bson.M{"$lte": time.Now().UTC()}}
	opts := options.Find().
		SetSort(bson.D{{Key: "when", Value: 1}}). // Ascending order (oldest first)
		SetLimit(int64(limit))

	cursor, err := b.collection.Find(readCtx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(readCtx)

	var messages []timebridge.StoredMessage
	for cursor.Next(readCtx) {
		var doc MessageDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		headers := make([]timebridge.Header, len(doc.Headers))
		for i, h := range doc.Headers {
			headers[i] = timebridge.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}

		messages = append(messages, timebridge.StoredMessage{
			Message: timebridge.Message{
				Key:     doc.Key,
				Value:   doc.Value,
				Headers: headers,
				When:    doc.When.UTC(), // Ensure UTC
				Where:   doc.Where,
			},
			Key: doc.ID,
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	deleteCtx, cancel := context.WithTimeout(ctx, time.Duration(b.cfg.DeleteTimeout)*time.Second)
	defer cancel()

	filter := bson.M{"_id": key}
	_, err := b.collection.DeleteOne(deleteCtx, filter)
	return err
}

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
	ID           string          `bson:"_id,omitempty"`
	Key          []byte          `bson:"key"`
	Value        []byte          `bson:"value"`
	Headers      []MessageHeader `bson:"headers"`
	When         time.Time       `bson:"when"`
	Where        string          `bson:"where"`
	ClaimedUntil time.Time       `bson:"claimed_until"` // zero = unclaimed; future = claimed; past = re-claimable
	ClaimedBy    string          `bson:"claimed_by"`    // batch claim UUID, "" when unclaimed
}

type Backend struct {
	cfg        timebridge.MongoDBConfig
	client     *mongo.Client
	collection *mongo.Collection
}

func NewBackend(cfg timebridge.MongoDBConfig) (*Backend, error) {
	return &Backend{cfg: cfg}, nil
}

func (b *Backend) Connect(ctx context.Context) error {

	// Create client options with explicit timeouts
	clientOptions := options.Client().
		ApplyURI(b.cfg.ConnectionString).
		SetConnectTimeout(time.Duration(b.cfg.ConnectTimeout) * time.Second).
		SetServerSelectionTimeout(time.Duration(b.cfg.ConnectTimeout) * time.Second)

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

	// Create indexes for efficient queries (if enabled)
	if b.cfg.AutoCreateIndex {
		indexCtx, indexCancel := context.WithTimeout(ctx, time.Duration(b.cfg.IndexTimeout)*time.Second)
		defer indexCancel()

		_, err = b.collection.Indexes().CreateMany(indexCtx, []mongo.IndexModel{
			// Hot-path: find unclaimed/re-claimable messages ready for delivery
			{Keys: bson.D{{Key: "when", Value: 1}, {Key: "claimed_until", Value: 1}}},
		})
		if err != nil {
			return err
		}
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
		// ClaimedUntil and ClaimedBy are zero values — document is unclaimed by default
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

	now := time.Now().UTC()
	claimToken := uuid.New().String()
	claimTTL := time.Duration(b.cfg.ClaimTTLSeconds) * time.Second

	// Step 1: Find candidate _ids — messages ready for delivery and not actively claimed.
	// claimed_until <= now covers both unclaimed (zero time, always <= now) and expired claims.
	filter := bson.D{
		{Key: "when", Value: bson.D{{Key: "$lte", Value: now}}},
		{Key: "claimed_until", Value: bson.D{{Key: "$lte", Value: now}}},
	}
	cursor, err := b.collection.Find(readCtx, filter,
		options.Find().
			SetSort(bson.D{{Key: "when", Value: 1}}).
			SetLimit(int64(limit)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}

	var candidateIDs []string
	for cursor.Next(readCtx) {
		var row struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&row); err != nil {
			cursor.Close(readCtx)
			return nil, err
		}
		candidateIDs = append(candidateIDs, row.ID)
	}
	cursor.Close(readCtx)
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	if len(candidateIDs) == 0 {
		return nil, nil
	}

	// Step 2: Atomically claim the candidates. The filter re-checks claimed_until <= now
	// so concurrent instances claiming the same IDs each win a disjoint subset.
	claimedUntil := now.Add(claimTTL)
	_, err = b.collection.UpdateMany(readCtx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: candidateIDs}}},
			{Key: "claimed_until", Value: bson.D{{Key: "$lte", Value: now}}},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "claimed_until", Value: claimedUntil},
			{Key: "claimed_by", Value: claimToken},
		}}},
	)
	if err != nil {
		return nil, err
	}

	// Step 3: Fetch what this instance claimed (those with our claimToken), ordered by when ASC.
	claimedCursor, err := b.collection.Find(readCtx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: candidateIDs}}},
			{Key: "claimed_by", Value: claimToken},
		},
		options.Find().SetSort(bson.D{{Key: "when", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer claimedCursor.Close(readCtx)

	var messages []timebridge.StoredMessage
	for claimedCursor.Next(readCtx) {
		var doc MessageDocument
		if err := claimedCursor.Decode(&doc); err != nil {
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
				When:    doc.When.UTC(),
				Where:   doc.Where,
			},
			Key: doc.ID,
		})
	}

	if err := claimedCursor.Err(); err != nil {
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

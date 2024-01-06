package db

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoDBBatch struct {
	db     *MongoDB
	batch  []mongo.WriteModel
	closed bool

	mu sync.Mutex
}

var _ Batch = (*mongoDBBatch)(nil)

func newMongoDBBatch(db *MongoDB) *mongoDBBatch {
	return &mongoDBBatch{
		db:     db,
		batch:  make([]mongo.WriteModel, 0),
		closed: false,
	}
}

// Set implements Batch.
func (b *mongoDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	if value == nil {
		return errValueNil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errBatchClosed
	}

	b.batch = append(b.batch,
		mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: string(key)}}).
			SetUpdate(bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: value}}}}).
			SetUpsert(true),
	)
	return nil
}

func (b *mongoDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errBatchClosed
	}

	b.batch = append(b.batch, mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: string(key)}}))
	return nil
}

func (b *mongoDBBatch) Write() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errBatchClosed
	}

	if len(b.batch) > 0 {
		if _, err := b.db.collection.BulkWrite(context.Background(), b.batch); err != nil {
			return err
		}
	}

	return b.closeUnsafe()
}

func (b *mongoDBBatch) WriteSync() error {
	return b.Write()
}

func (b *mongoDBBatch) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.closeUnsafe()
}

func (b *mongoDBBatch) closeUnsafe() error {
	b.closed = true
	return nil
}

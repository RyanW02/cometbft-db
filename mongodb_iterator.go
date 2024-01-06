package db

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

type mongoDBIterator struct {
	db     *MongoDB
	cursor *mongo.Cursor

	start, end []byte

	lastErr error
	item    *record

	mu sync.Mutex
}

var _ Iterator = (*mongoDBIterator)(nil)

func newMongoDBIterator(db *MongoDB, start, end []byte, isReverse bool) (*mongoDBIterator, error) {
	var filter bson.D
	if start == nil && end == nil {
		filter = bson.D{}
	} else {
		filterArray := bson.A{}
		if start != nil {
			filterArray = append(filterArray, bson.D{{"_id", bson.D{{"$gte", start}}}})
		}

		if end != nil {
			filterArray = append(filterArray, bson.D{{"_id", bson.D{{"$lt", end}}}})
		}

		filter = bson.D{{"$and", filterArray}}
	}

	var opts *options.FindOptions
	if isReverse {
		opts = options.Find().SetSort(bson.D{{"_id", -1}})
	} else {
		opts = options.Find().SetSort(bson.D{{"_id", 1}})
	}

	cursor, err := db.collection.Find(context.Background(), filter, opts)
	if err != nil {
		return nil, err
	}

	return &mongoDBIterator{
		db:     db,
		cursor: cursor,
		start:  start,
		end:    end,
	}, nil
}

// Domain implements Iterator.
func (itr *mongoDBIterator) Domain() ([]byte, []byte) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	return itr.start, itr.end
}

func (itr *mongoDBIterator) Valid() bool {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if !itr.cursor.Next(context.Background()) {
		return false
	}

	var record record
	if err := itr.cursor.Decode(&record); err != nil {
		itr.lastErr = err
		return false
	}

	itr.item = &record
	return true
}

func (itr *mongoDBIterator) Next() {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	itr.item = nil
}

func (itr *mongoDBIterator) Key() (key []byte) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.item == nil {
		panic("invalid iterator: current is nil - call Next() first")
	}

	return itr.item.Key
}

func (itr *mongoDBIterator) Value() (value []byte) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.item == nil {
		panic("invalid iterator: current is nil - call Next() first")
	}

	return itr.item.Value
}

func (itr *mongoDBIterator) Error() error {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	return itr.lastErr
}

func (itr *mongoDBIterator) Close() error {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	return itr.cursor.Close(context.Background())
}

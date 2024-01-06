package db

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

type mongoDBIterator struct {
	db     *MongoDB
	cursor *mongo.Cursor

	start, end []byte

	lastErr       error
	current, next *record

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
			if len(start) == 0 {
				return nil, errKeyEmpty
			}

			filterArray = append(filterArray, bson.D{{"_id", bson.D{{"$gte", start}}}})
		}

		if end != nil {
			if len(end) == 0 {
				return nil, errKeyEmpty
			}

			filterArray = append(filterArray, bson.D{{"_id", bson.D{{"$lt", end}}}})
		}

		filter = bson.D{{"$and", filterArray}}
	}

	fmt.Println(filter)

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

	it := &mongoDBIterator{
		db:     db,
		cursor: cursor,
		start:  start,
		end:    end,
	}

	// Load current and next records
	if !cursor.Next(context.Background()) {
		return it, nil
	}

	if err := it.cursor.Decode(&it.current); err != nil {
		return nil, err
	}

	if !cursor.Next(context.Background()) {
		return it, nil
	}

	if err := it.cursor.Decode(&it.next); err != nil {
		return nil, err
	}

	return it, nil
}

// Domain implements Iterator.
func (it *mongoDBIterator) Domain() ([]byte, []byte) {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.start, it.end
}

func (it *mongoDBIterator) Valid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.current != nil
}

func (it *mongoDBIterator) Next() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.current == nil {
		panic("invalid iterator: current is nil - call Valid() first")
	}

	it.current = it.next
	it.next = nil

	// Load next record
	if !it.cursor.Next(context.Background()) {
		return
	}

	var record record
	if err := it.cursor.Decode(&record); err != nil {
		it.lastErr = err
		return
	}

	it.next = &record
}

func (it *mongoDBIterator) Key() (key []byte) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.current == nil {
		panic("invalid iterator: current is nil - call Next() first")
	}

	return it.current.Key
}

func (it *mongoDBIterator) Value() (value []byte) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.current == nil {
		panic("invalid iterator: current is nil - call Next() first")
	}

	return it.current.Value
}

func (it *mongoDBIterator) Error() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.lastErr
}

func (it *mongoDBIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.cursor.Close(context.Background())
}

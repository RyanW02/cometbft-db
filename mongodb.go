package db

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
)

func init() { registerDBCreator(MongoDBBackend, mongoDBCreator, true) }

func mongoDBCreator(options Options) (DB, error) {
	connString, ok := options.GetString("connection_string")
	if !ok {
		return nil, errors.New("connection_string not provided in options")
	}

	databaseName, ok := options.GetString("database")
	if !ok {
		return nil, errors.New("database not provided in options")
	}

	collectionName, ok := options.GetString("collection")
	if !ok {
		return nil, errors.New("collection not provided in options")
	}

	serverAPI := mongoOptions.ServerAPI(mongoOptions.ServerAPIVersion1)
	opts := mongoOptions.Client().ApplyURI(connString).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	collection := client.Database(databaseName).Collection(collectionName)
	return NewMongoDB(collection), nil
}

func NewMongoDBOptions(connectionString, database, collection string) Options {
	return Options{
		"connection_string": connectionString,
		"database":          database,
		"collection":        collection,
	}
}

type MongoDB struct {
	collection *mongo.Collection
}

// Compile time verification of interface implementation
var _ DB = (*MongoDB)(nil)

// NewMongoDB creates a new CometBFT MongoDB wrapper.
func NewMongoDB(collection *mongo.Collection) *MongoDB {
	return &MongoDB{
		collection: collection,
	}
}

// Struct representing a record in the MongoDB collection.
type record struct {
	Key   []byte `bson:"_id"`
	Value []byte `bson:"value"`
}

// Constructor for a record.
func newRecord(key, value []byte) record {
	return record{
		Key:   key,
		Value: value,
	}
}

// Get fetches a value from the database by key.
// Returns (nil, nil) if the key does not exist.
func (db *MongoDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	res := db.collection.FindOne(context.Background(), bson.D{{"_id", string(key)}})
	if res.Err() != nil {
		if errors.Is(res.Err(), mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, res.Err()
	}

	var record record
	if err := res.Decode(&record); err != nil {
		return nil, err
	}

	return record.Value, nil
}

// Has checks if a key exists in the database.
func (db *MongoDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errKeyEmpty
	}

	res := db.collection.FindOne(context.Background(), bson.D{{"_id", string(key)}})
	if res.Err() != nil {
		if errors.Is(res.Err(), mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, res.Err()
	}

	return true, nil
}

// Set inserts a key-value pair into the database. If the key already exists, the value is overwritten.
func (db *MongoDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	if value == nil {
		return errValueNil
	}

	_, err := db.collection.UpdateOne(
		context.Background(),
		bson.D{{"_id", string(key)}},
		bson.D{{"$set", bson.D{{"value", value}}}},
		&mongoOptions.UpdateOptions{Upsert: ptr(true)},
	)
	return err
}

// SetSync has the same functionality as Set. The MongoDB driver handles synchronization.
func (db *MongoDB) SetSync(key, value []byte) error {
	return db.Set(key, value)
}

// Delete removes a key-value pair from the database, if it exists.
func (db *MongoDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	_, err := db.collection.DeleteOne(context.Background(), bson.D{{"_id", string(key)}})
	return err
}

// DeleteSync has the same functionality as Delete. The MongoDB driver handles synchronization.
func (db *MongoDB) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Iterator returns an iterator over a domain of keys, in ascending order. Close() must be called when done.
// Start is inclusive, and end is exclusive.
// Example usage:
//
//		itr, err := db.Iterator(start, end)
//	 ...
//		defer itr.Close()
//		for ; itr.Valid(); itr.Next() {
//			key := itr.Key()
//			value := itr.Value()
//			...
//		}
func (db *MongoDB) Iterator(start, end []byte) (Iterator, error) {
	return newMongoDBIterator(db, start, end, false)
}

// ReverseIterator returns an iterator over a domain of keys, in descending order. Close() must be called when done.
// Start is exclusive, and end is inclusive.
// Example usage:
//
//		itr, err := db.ReverseIterator(start, end)
//	 ...
//		defer itr.Close()
//		for ; itr.Valid(); itr.Next() {
//			key := itr.Key()
//			value := itr.Value()
//			...
//		}
func (db *MongoDB) ReverseIterator(start, end []byte) (Iterator, error) {
	return newMongoDBIterator(db, start, end, true)
}

// Close closes the underlying MongoDB client.
func (db *MongoDB) Close() error {
	return db.collection.Database().Client().Disconnect(context.Background())
}

// NewBatch returns a new write batch for the database. Batch.Write() must be called to commit the batch.
func (db *MongoDB) NewBatch() Batch {
	return newMongoDBBatch(db)
}

// Print prints debug information about the database. This should not be used in production.
func (db *MongoDB) Print() error {
	stats := db.Stats()
	fmt.Println("Stats:")
	for key, value := range stats {
		fmt.Printf("%s:\t%s\n", key, value)
	}

	return nil
}

// Stats returns a map of property values provided by the collStats MongoDB command.
func (db *MongoDB) Stats() map[string]string {
	result := db.collection.Database().RunCommand(
		context.Background(),
		bson.M{"collStats": db.collection.Name()},
	)

	if result.Err() != nil {
		return map[string]string{"error": result.Err().Error()}
	}

	var document bson.M
	if err := result.Decode(&document); err != nil {
		return map[string]string{"error": err.Error()}
	}

	stats := make(map[string]string)
	for key, value := range document {
		stats[key] = fmt.Sprintf("%v", value)
	}

	return stats
}

package db

import (
	"context"
	"fmt"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

type MongoTestSuite struct {
	suite.Suite

	db       DB
	client   *mongo.Client
	pool     *dockertest.Pool
	resource *dockertest.Resource
}

func TestMongo(t *testing.T) {
	suite.Run(t, new(MongoTestSuite))
}

func (s *MongoTestSuite) SetupSuite() {
	s.T().Log("Connecting to Docker...")
	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(err)
	}

	if err := pool.Client.Ping(); err != nil {
		panic(err)
	}

	s.T().Log("Connected to Docker, starting MongoDB container...")

	client, resource, err := s.setupMongoDb(pool)
	if err != nil {
		panic(err)
	}

	collection := client.Database("testing").Collection("testing")
	if _, err := collection.DeleteMany(context.Background(), bson.D{}); err != nil {
		panic(err)
	}

	db := NewMongoDB(collection)

	s.db = db
	s.client = client
	s.pool = pool
	s.resource = resource
}

func (s *MongoTestSuite) TearDownSuite() {
	if err := s.client.Disconnect(context.Background()); err != nil {
		panic(err)
	}

	if err := s.pool.Purge(s.resource); err != nil {
		panic(err)
	}
}

func (s *MongoTestSuite) TearDownTest() {
	_, err := s.client.Database("testing").Collection("testing").DeleteMany(context.Background(), bson.D{})
	if err != nil {
		panic(err)
	}
}

func (s *MongoTestSuite) setupMongoDb(pool *dockertest.Pool) (*mongo.Client, *dockertest.Resource, error) {
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "7",
		Env: []string{
			"MONGO_INITDB_ROOT_USERNAME=root",
			"MONGO_INITDB_ROOT_PASSWORD=password",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		return nil, nil, err
	}

	s.T().Log("MongoDB container started, waiting for it to be ready...")

	var client *mongo.Client
	if err := pool.Retry(func() error {
		var err error
		client, err = mongo.Connect(
			context.TODO(),
			options.Client().ApplyURI(
				fmt.Sprintf("mongodb://root:password@localhost:%s", resource.GetPort("27017/tcp")),
			),
		)
		if err != nil {
			return err
		}

		return client.Ping(context.TODO(), nil)
	}); err != nil {
		return nil, nil, err
	}

	s.T().Log("MongoDB container ready")

	return client, resource, nil
}

func (s *MongoTestSuite) TestDatabaseOnline() {
	assert.NoError(s.T(), s.client.Ping(context.Background(), nil))
}

func (s *MongoTestSuite) TestGetNonExistent() {
	value, err := s.db.Get([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error getting key1") {
		assert.Equalf(s.T(), []byte(nil), value, "value of key1")
		assert.True(s.T(), value == nil)
	}
}

func (s *MongoTestSuite) TestGetEmptyKey() {
	assert.ErrorIs(s.T(), s.db.Set([]byte(""), []byte("value")), errKeyEmpty)
}

func (s *MongoTestSuite) TestHasNonExistent() {
	exists, err := s.db.Has([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error checking key1") {
		assert.Falsef(s.T(), exists, "key1 exists")
	}
}

func (s *MongoTestSuite) TestHasExists() {
	if assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1") {
		exists, err := s.db.Has([]byte("key1"))
		if assert.NoErrorf(s.T(), err, "error checking key1") {
			assert.Truef(s.T(), exists, "key1 does not exist")
		}
	}
}

func (s *MongoTestSuite) TestHasEmptyKey() {
	_, err := s.db.Has([]byte(""))
	assert.ErrorIs(s.T(), err, errKeyEmpty)
}

func (s *MongoTestSuite) TestSet() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value123")), "error setting key1")
	value, err := s.db.Get([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error getting key1") {
		assert.Equalf(s.T(), []byte("value123"), value, "value of key1")
	}
}

func (s *MongoTestSuite) TestSetEmptyKey() {
	assert.ErrorIs(s.T(), s.db.Set([]byte(""), []byte("value")), errKeyEmpty)
}

func (s *MongoTestSuite) TestSetNilValue() {
	assert.ErrorIs(s.T(), s.db.Set([]byte("key"), nil), errValueNil)
}

func (s *MongoTestSuite) TestSetSync() {
	assert.NoErrorf(s.T(), s.db.SetSync([]byte("key1"), []byte("value123")), "error setting key1")
	value, err := s.db.Get([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error getting key1") {
		assert.Equalf(s.T(), []byte("value123"), value, "value of key1")
	}
}

func (s *MongoTestSuite) TestSetSyncEmptyKey() {
	assert.ErrorIs(s.T(), s.db.SetSync([]byte(""), []byte("value")), errKeyEmpty)
}

func (s *MongoTestSuite) TestSetSyncNilValue() {
	assert.ErrorIs(s.T(), s.db.SetSync([]byte("key"), nil), errValueNil)
}

func (s *MongoTestSuite) TestDeleteNonExistent() {
	assert.NoErrorf(s.T(), s.db.Delete([]byte("key1")), "error deleting key1")
}

func (s *MongoTestSuite) TestDeleteEmptyKey() {
	assert.ErrorIs(s.T(), s.db.Delete([]byte("")), errKeyEmpty)
}

func (s *MongoTestSuite) TestDelete() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value123")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Delete([]byte("key1")), "error deleting key1")

	exists, err := s.db.Has([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error checking key1") {
		assert.Falsef(s.T(), exists, "key1 exists")
	}
}

func (s *MongoTestSuite) TestDeleteSyncNonExistent() {
	assert.NoErrorf(s.T(), s.db.DeleteSync([]byte("key1")), "error deleting key1")
}

func (s *MongoTestSuite) TestDeleteSyncEmptyKey() {
	assert.ErrorIs(s.T(), s.db.DeleteSync([]byte("")), errKeyEmpty)
}

func (s *MongoTestSuite) TestDeleteSync() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value123")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.DeleteSync([]byte("key1")), "error deleting key1")

	exists, err := s.db.Has([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error checking key1") {
		assert.Falsef(s.T(), exists, "key1 exists")
	}
}

func (s *MongoTestSuite) TestIterator() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	// End is exclusive, so key4 is not included.
	it, err := s.db.Iterator([]byte("key1"), []byte("key4"))
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestIteratorAll() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("abc"), []byte("value3")), "error setting abc")

	it, err := s.db.Iterator(nil, nil)
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"abc":  "value3",
			"key1": "value1",
			"key2": "value2",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestIteratorStartOnly() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	it, err := s.db.Iterator([]byte("key2"), nil)
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestIteratorEndOnly() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	// End is exclusive, so key3 and key4 is not included.
	it, err := s.db.Iterator(nil, []byte("key3"))
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"key1": "value1",
			"key2": "value2",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestIteratorOrder() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	it, err := s.db.Iterator(nil, nil)
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		i := 1
		for ; it.Valid(); it.Next() {
			assert.Equalf(s.T(), fmt.Sprintf("key%d", i), string(it.Key()), "iterator key")
			assert.Equalf(s.T(), fmt.Sprintf("value%d", i), string(it.Value()), "iterator value")
			i++
		}
	}
}

func (s *MongoTestSuite) TestReverseIterator() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	// End is exclusive, so key4 is not included.
	it, err := s.db.ReverseIterator([]byte("key1"), []byte("key4"))
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestReverseIteratorAll() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("abc"), []byte("value3")), "error setting abc")

	it, err := s.db.ReverseIterator(nil, nil)
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"abc":  "value3",
			"key1": "value1",
			"key2": "value2",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestReverseIteratorStartOnly() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	it, err := s.db.ReverseIterator([]byte("key2"), nil)
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestReverseIteratorEndOnly() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	// End is exclusive, so key3 and key4 is not included.
	it, err := s.db.ReverseIterator(nil, []byte("key3"))
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		m := make(map[string]string)
		for ; it.Valid(); it.Next() {
			m[string(it.Key())] = string(it.Value())
		}

		assert.Equalf(s.T(), map[string]string{
			"key1": "value1",
			"key2": "value2",
		}, m, "iterator values")
	}
}

func (s *MongoTestSuite) TestReverseIteratorOrder() {
	assert.NoErrorf(s.T(), s.db.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), s.db.Set([]byte("key4"), []byte("value4")), "error setting key4")

	it, err := s.db.ReverseIterator(nil, nil)
	if assert.NoErrorf(s.T(), err, "error creating iterator") {
		defer it.Close()

		i := 4
		for ; it.Valid(); it.Next() {
			assert.Equalf(s.T(), fmt.Sprintf("key%d", i), string(it.Key()), "iterator key")
			assert.Equalf(s.T(), fmt.Sprintf("value%d", i), string(it.Value()), "iterator value")
			i--
		}
	}
}

func (s *MongoTestSuite) TestBatchWrites() {
	batch := s.db.NewBatch()
	assert.NoErrorf(s.T(), batch.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), batch.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), batch.Set([]byte("key3"), []byte("value3")), "error setting key3")

	assert.NoErrorf(s.T(), batch.Write(), "error writing batch")

	value, err := s.db.Get([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error getting key1") {
		assert.Equalf(s.T(), []byte("value1"), value, "value of key1")
	}

	value, err = s.db.Get([]byte("key2"))
	if assert.NoErrorf(s.T(), err, "error getting key2") {
		assert.Equalf(s.T(), []byte("value2"), value, "value of key2")
	}

	value, err = s.db.Get([]byte("key3"))
	if assert.NoErrorf(s.T(), err, "error getting key3") {
		assert.Equalf(s.T(), []byte("value3"), value, "value of key3")
	}
}

func (s *MongoTestSuite) TestBatchWriteAndDelete() {
	batch := s.db.NewBatch()
	assert.NoErrorf(s.T(), batch.Set([]byte("key1"), []byte("value1")), "error setting key1")
	assert.NoErrorf(s.T(), batch.Set([]byte("key2"), []byte("value2")), "error setting key2")
	assert.NoErrorf(s.T(), batch.Set([]byte("key3"), []byte("value3")), "error setting key3")
	assert.NoErrorf(s.T(), batch.Delete([]byte("key2")), "error deleting key2")

	assert.NoErrorf(s.T(), batch.Write(), "error writing batch")

	value, err := s.db.Get([]byte("key1"))
	if assert.NoErrorf(s.T(), err, "error getting key1") {
		assert.Equalf(s.T(), []byte("value1"), value, "value of key1")
	}

	exists, err := s.db.Has([]byte("key2"))
	if assert.NoErrorf(s.T(), err, "error getting key2") {
		assert.Falsef(s.T(), exists, "key2 exists")
	}

	value, err = s.db.Get([]byte("key3"))
	if assert.NoErrorf(s.T(), err, "error getting key3") {
		assert.Equalf(s.T(), []byte("value3"), value, "value of key3")
	}
}

func (s *MongoTestSuite) TestBatchCloseIdempotent() {
	batch := s.db.NewBatch()
	assert.NoErrorf(s.T(), batch.Write(), "error writing batch")
	assert.NoErrorf(s.T(), batch.Close(), "error closing batch")
	assert.NoErrorf(s.T(), batch.Close(), "error closing batch")
}

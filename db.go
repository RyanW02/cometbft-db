package db

import (
	"errors"
	"fmt"
	"strings"
)

type BackendType string

// These are valid backend types.
const (
	// GoLevelDBBackend represents goleveldb (github.com/syndtr/goleveldb - most
	// popular implementation)
	//   - pure go
	//   - stable
	GoLevelDBBackend BackendType = "goleveldb"
	// CLevelDBBackend represents cleveldb (uses levigo wrapper)
	//   - fast
	//   - requires gcc
	//   - use cleveldb build tag (go build -tags cleveldb)
	CLevelDBBackend BackendType = "cleveldb"
	// MemDBBackend represents in-memory key value store, which is mostly used
	// for testing.
	MemDBBackend BackendType = "memdb"
	// BoltDBBackend represents bolt (uses etcd's fork of bolt -
	// github.com/etcd-io/bbolt)
	//   - EXPERIMENTAL
	//   - may be faster is some use-cases (random reads - indexer)
	//   - use boltdb build tag (go build -tags boltdb)
	BoltDBBackend BackendType = "boltdb"
	// RocksDBBackend represents rocksdb (uses github.com/tecbot/gorocksdb)
	//   - EXPERIMENTAL
	//   - requires gcc
	//   - use rocksdb build tag (go build -tags rocksdb)
	RocksDBBackend BackendType = "rocksdb"

	BadgerDBBackend BackendType = "badgerdb"

	// MongoDBBackend represents a remote (i.e. not connected via a network
	// or unix socket) MongoDB server.
	MongoDBBackend BackendType = "mongodb"
)

type Options map[string]interface{}

func (o Options) GetString(key string) (string, bool) {
	val, ok := o[key]
	if !ok {
		return "", false
	}

	str, ok := val.(string)
	if !ok {
		return "", false
	}

	return str, true
}

type dbCreator func(options Options) (DB, error)

var backends = map[BackendType]dbCreator{}

func registerDBCreator(backend BackendType, creator dbCreator, force bool) {
	_, ok := backends[backend]
	if !force && ok {
		return
	}
	backends[backend] = creator
}

// NewDB creates a new database of type backend with the given name.
func NewDB(backend BackendType, options Options) (DB, error) {
	dbCreator, ok := backends[backend]
	if !ok {
		keys := make([]string, 0, len(backends))
		for k := range backends {
			keys = append(keys, string(k))
		}
		return nil, fmt.Errorf("unknown db_backend %s, expected one of %v",
			backend, strings.Join(keys, ","))
	}

	db, err := dbCreator(options)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	return db, nil
}

const optionName = "name"
const optionDir = "dir"

var errMissingOption = errors.New("missing option")

// NewFlatFileDB is a shortcut for NewDB for flat-file backends.
func NewFlatFileDB(name string, backend BackendType, dir string) (DB, error) {
	options := Options{
		optionName: name,
		optionDir:  dir,
	}

	return NewDB(backend, options)
}

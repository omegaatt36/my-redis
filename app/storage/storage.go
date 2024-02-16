package storage

import (
	"fmt"
	"sync"
	"time"
)

// ErrKeyNotFound is returned when a key is not found in the database.
var ErrKeyNotFound = fmt.Errorf("key not found")

// Store is an interface for a simple key-value store.
type Store interface {
	Get(key string) (string, error)
	Set(key string, value string, expiredAt *time.Time) error
	Del(key string) error
}

// Database defines a simple in-memory key-value store.
type Database struct {
	kv map[string]string

	mutex sync.Mutex
}

// NewDatabase creates a new Database.
func NewDatabase() *Database {
	return &Database{
		kv: make(map[string]string),
	}
}

// Get retrieves a value from the database.
func (db *Database) Get(key string) (string, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	value, ok := db.kv[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}

// Set sets a value in the database.
func (db *Database) Set(key string, value string, expiredAt *time.Time) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if expiredAt != nil {
		go func() {
			now := time.Now().In(time.UTC)
			if expiredAt.Before(now) {
				db.Del(key)
			} else {
				<-time.After(expiredAt.Sub(now))
				db.Del(key)
			}
		}()
	}

	db.kv[key] = value
	return nil
}

// Del deletes a value from the database.
func (db *Database) Del(key string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	delete(db.kv, key)

	return nil
}

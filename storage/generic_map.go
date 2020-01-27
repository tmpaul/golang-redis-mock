// Package storage takes care of storing the actual values in memory
package storage

import (
	"sync"
	"time"
)

// The following concurrent map implementation is based on the following source:
// https://medium.com/@deckarep/the-new-kid-in-town-gos-sync-map-de24a6bf7c2c

// GenericConcurrentMap maps a string key to a int or string value
type GenericConcurrentMap struct {
	sync.RWMutex
	internal map[string]string
	eq       *ExpiryQueue
}

// NewGenericConcurrentMap creates a new string > int or string map
func NewGenericConcurrentMap() *GenericConcurrentMap {
	eq := NewExpiryQueue()
	gm := GenericConcurrentMap{
		internal: make(map[string]string),
		eq:       eq,
	}
	go gm.expireKey(eq.out)
	return &gm
}

func (gcm *GenericConcurrentMap) expireKey(out chan string) {
	for {
		key := <-out
		gcm.Delete(key)
	}
}

// SetExpiry sets the expiry value for key.
func (gcm *GenericConcurrentMap) SetExpiry(key string, ttl int64) {
	currSec := time.Now().Unix()
	// Add current time in seconds, so that we use absolute number of seconds since epoch
	gcm.eq.insertKey(key, currSec+ttl)
}

// Load a new value from the map or nil, if it does not exist
func (gcm *GenericConcurrentMap) Load(key string) (value string, ok bool) {
	gcm.RLock()
	defer gcm.RUnlock()
	result, ok := gcm.internal[key]
	return result, ok
}

// Delete value at a given key, and returns true if deleted, false otherwise
func (gcm *GenericConcurrentMap) Delete(key string) bool {
	gcm.Lock()
	defer gcm.Unlock()
	_, ok := gcm.internal[key]
	if ok == false {
		return false
	}
	// Delete is a no-op if key does not exist. Without a lock, we may end up deleting
	// an item that is not written or vice. We use a return value explicitly by invoking
	// a read. Since the read is performed after a lock, we are okay
	delete(gcm.internal, key)
	return true
}

// Store a given int or string value at given key
func (gcm *GenericConcurrentMap) Store(key string, value string) {
	gcm.Lock()
	defer gcm.Unlock()
	gcm.internal[key] = value
}

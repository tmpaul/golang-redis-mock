// The storage package takes care of storing the actual values in memory
// Based on source: https://medium.com/@deckarep/the-new-kid-in-town-gos-sync-map-de24a6bf7c2c

package storage

import (
	"fmt"
	"sync"
)

// GCMIntegerType extends int type
type GCMIntegerType struct {
	value int
}

// GCMStringType extends string type
type GCMStringType struct {
	value string
}

// IntegerOrString defines an interface that allows us to tag both string and int types
type IntegerOrString interface {
	amIStringOrInteger() bool
}

func (GCMIntegerType) amIStringOrInteger() bool {
	return true
}

func (g *GCMIntegerType) GetValue() int {
	return g.value
}

func (GCMStringType) amIStringOrInteger() bool {
	return true
}

func (g *GCMStringType) GetValue() string {
	return g.value
}

// GenericConcurrentMap maps a string key to a int or string value
type GenericConcurrentMap struct {
	sync.RWMutex
	internal map[string]IntegerOrString
}

// NewGenericConcurrentMap creates a new string > int or string map
func NewGenericConcurrentMap() *GenericConcurrentMap {
	return &GenericConcurrentMap{
		internal: make(map[string]IntegerOrString),
	}
}

// Load a new value from the map or nil, if it does not exist
func (rm *GenericConcurrentMap) Load(key string) (value IntegerOrString, ok bool) {
	rm.RLock()
	defer rm.RUnlock()
	result, ok := rm.internal[key]
	return result, ok
}

// Delete value at a given key, and returns true if deleted, false otherwise
func (rm *GenericConcurrentMap) Delete(key string) bool {
	rm.Lock()
	defer rm.Unlock()
	_, ok := rm.internal[key]
	if ok == false {
		return false
	}
	// Delete is a no-op if key does not exist. Without a lock, we may end up deleting
	// an item that is not written or vice. We use a return value explicitly by invoking
	// a read. Since the read is performed after a lock, we are okay
	delete(rm.internal, key)
	fmt.Println("Deleting")
	return true
}

// Store a given int or string value at given key
func (rm *GenericConcurrentMap) Store(key string, value IntegerOrString) {
	rm.Lock()
	defer rm.Unlock()
	fmt.Println("Storing")
	rm.internal[key] = value
}

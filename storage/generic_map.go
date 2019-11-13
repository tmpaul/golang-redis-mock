// Package storage takes care of storing the actual values in memory
package storage

import (
	"sync"
)

// Here we try to implement a single map that can store an integer or a string.
// The goal is to understand how we can use an interface to store values of different underlying
// types within the same structure.

// Pros >
// 1. Single map to store
// 2. Single interface makes it easy

// Cons >
// 1. Types are wrapped. Any usage would require unwrapping with type switch.
// 2. Cannot implement integer specific operations easily.

// For alternate implementations, see ConcurrentIntegerMap and ConcurrentStringMap

// The following concurrent map implementation is based on the following source:
// https://medium.com/@deckarep/the-new-kid-in-town-gos-sync-map-de24a6bf7c2c

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

// GetValue returns underlying integer value
func (g *GCMIntegerType) GetValue() int {
	return g.value
}

// NewGCMInteger creates a new GCMInteger value
func NewGCMInteger(i int) *GCMIntegerType {
	return &GCMIntegerType{i}
}

// NewGCMString creates a new GCMInteger value
func NewGCMString(s string) *GCMStringType {
	return &GCMStringType{s}
}

func (GCMStringType) amIStringOrInteger() bool {
	return true
}

// GetValue returns underlying string value
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
	return true
}

// Store a given int or string value at given key
func (rm *GenericConcurrentMap) Store(key string, value IntegerOrString) {
	rm.Lock()
	defer rm.Unlock()
	rm.internal[key] = value
}

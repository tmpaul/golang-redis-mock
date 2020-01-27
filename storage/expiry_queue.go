package storage

import (
	"sort"
	"sync"
	"time"
)

// ExpiryQueue keeps track of keys that will expire within a given threshold (seconds)
// It takes care of deleting maps
type ExpiryQueue struct {
	ttlMap map[string]int64
	// Keep keys sorted so that ones that will expire first can be checked soon
	// This allows us to break early
	sortedKeys []string
	// Write to this channel when a key is expired
	out chan string
	// A mutex to make sure that sortedKeys is not modified concurrently
	mux sync.Mutex
}

// NewExpiryQueue creates a new expiry queue
func NewExpiryQueue() *ExpiryQueue {
	eq := ExpiryQueue{
		ttlMap:     make(map[string]int64),
		sortedKeys: make([]string, 0),
		out:        make(chan string),
	}
	go eq.expireKeys()
	return &eq
}

// Insert a key into expiry queue
func (eq *ExpiryQueue) insertKey(x string, ttl int64) {
	eq.mux.Lock()
	defer eq.mux.Unlock()
	data := eq.sortedKeys
	vMap := eq.ttlMap
	eq.ttlMap[x] = ttl
	i := sort.Search(len(data), func(i int) bool { return vMap[data[i]] >= ttl })
	data = append(data, "")
	copy(data[i+1:], data[i:])
	data[i] = x
	eq.sortedKeys = data
}

// Expire the keys in a timed loop with intervals of 1 second
func (eq *ExpiryQueue) expireKeys() {
	currSec := time.Now().Unix()
	// Useful when listener goroutine is shutdown
	exitFlag := false
	for {
		// Start time
		start := time.Now().Unix()
		eq.mux.Lock()
		if len(eq.sortedKeys) != 0 {
			first := eq.sortedKeys[0]
			if eq.ttlMap[first] <= currSec {
				// Create a copy of keys so that we do not use the original array which might be modified
				// by the time we end
				keys := eq.sortedKeys
				till := 1
				// Get first key
				for i := 0; i < len(keys); i++ {
					k := keys[i]
					if eq.ttlMap[k] <= currSec {
						// Perform conditional write, if there are no listeners
						// log warning
						select {
						case eq.out <- k:
						default:
							// Here we break out of goroutine, because there are no more listeners left
							exitFlag = true
							break
						}
						delete(eq.ttlMap, k)
					} else {
						till = i
						break
					}
				}
				// Remove all keys upto i
				eq.sortedKeys = keys[till:]
			}
		}
		eq.mux.Unlock()
		// Sleep for 1 second
		time.Sleep(1 * time.Second)
		// This is approximate, if deleting takes more than 1 second because
		// the reader is blocked, we need to add difference
		currSec += time.Now().Unix() - start

		if exitFlag == true {
			break
		}
	}
}

package kvstore

import (
	"encoding/json"
	"kvstore/vectorclock"
	"sync"
	"sync/atomic"
	"time"
)

// CausalContext is how we are able to maintain causal
// consistency.
type CausalContext struct {
	Clock vectorclock.VectorClock `json:"clock"`
	Key   string                  `json:"key"`
}

// Value is a structure which is the entry into the
// hash table for the key-value store.
type Value struct {
	Timestamp time.Time               `json:"time"`
	Clock     vectorclock.VectorClock `json:"clock"`
	Depends   CausalContext           `json:"depends"`
	Available bool                    `json:"available"`
	Data      string                  `json:"data"`
}

// KVStore is a collection of hash tables for storing
// KV pairs, locks, dependency tables, etc.
type KVStore struct {
	data       map[string][]Value
	locks      map[string]*sync.RWMutex
	dependency map[string][]Value
	gcInUse    uint32
}

func (s *KVStore) isAvailable(value Value) bool {
	if value.Available {
		return true
	} else if value.Depends.Clock.ZeroClock() {
		return true
	}

	dependentCausality := value.Depends
	dependentValues, exists := s.data[dependentCausality.Key]
	if !exists {
		return false
	}
	allDependentValues, _ := s.traverseValues(dependentValues, dependentCausality.Clock, true)

	equalDependentValues := allDependentValues[vectorclock.Equal]
	if len(equalDependentValues) == 0 {
		return false
	}

	// traverseValues will only return available values since we specified as so.
	// Hence, if we reach this point, it means at least one equal value we depended
	// upon is available.
	value.Available = true
	return true
}

func (s *KVStore) traverseValues(values []Value, clock vectorclock.VectorClock, onlyAvailable bool) ([vectorclock.Max][]Value, [vectorclock.Max][]int) {
	var allValues [vectorclock.Max][]Value
	var allIndices [vectorclock.Max][]int

	for index, value := range values {
		if (!onlyAvailable) || s.isAvailable(value) {
			relation := value.Clock.Compare(clock)
			allValues[relation] = append(allValues[relation], value)
			allIndices[relation] = append(allIndices[relation], index)
		}
	}

	return allValues, allIndices
}

// If we add a hash table to maintain dependencies, this operation
// becomes trivially simple and quick.
//
// We already maintain a lock to the key in the context while this
// processing is done, so make sure we don't try to lock again. I know,
// this is probably terrible design.
func (s *KVStore) findDependencies(context CausalContext) ([]string, []int) {
	var keyList = make([]string, 0, 4)
	var indexList = make([]int, 0, 4)

	for key, values := range s.data {
		lock, _ := s.locks[key]
		if key != context.Key {
			lock.RLock()
		}

		for index, value := range values {
			dependency := value.Depends
			if dependency.Clock.Compare(context.Clock) == vectorclock.Equal && key == context.Key {
				keyList = append(keyList, key)
				indexList = append(indexList, index)
			}
		}

		if key != context.Key {
			lock.RUnlock()
		}
	}

	return keyList, indexList
}

// We already maintain a lock to the key in the context while this
// processing is done, so make sure we don't try to lock again. I know,
// this is probably terrible design.
func (s *KVStore) delete(key string, value Value, index int) {
	// Any keys which depend on this key have the same dependency clock
	// as this one.
	dependencyContext := CausalContext{Clock: value.Clock, Key: key}

	dependentKeys, dependentIndices := s.findDependencies(dependencyContext)
	for i, dependentKey := range dependentKeys {
		lock, _ := s.locks[dependentKey]
		if dependentKey != key {
			lock.Lock()
		}
		valueIndex := dependentIndices[i]

		// Firstly, save the value and modify it's dependency.
		dependentValue := s.data[dependentKey][valueIndex]
		dependentValue.Depends = value.Depends

		// Secondly, remove the old value from the list of values.
		s.data[dependentKey] = append(s.data[dependentKey][:valueIndex], s.data[dependentKey][valueIndex+1:]...)

		// Lastly, add the new value back in.
		s.data[dependentKey] = append(s.data[dependentKey], dependentValue)

		if dependentKey != key {
			lock.Unlock()
		}
	}

	// Now that all dependencies have been fixed, we can remove this value itself.
	s.data[key] = append(s.data[key][:index], s.data[key][index+1:]...)
}

func (s *KVStore) garbageCollection() {
	swapped := atomic.CompareAndSwapUint32(&s.gcInUse, 0, 1)
	if !swapped {
		return
	}

	for key, lock := range s.locks {
		lock.Lock()

		// The garbage collector should resolve all the values
		// for this key into a single one.
		//
		// All values which are causally older are removed and values
		// which are causally unrelated are compared through the stored
		// timestamp.
		allValues, _ := s.data[key]
		for len(allValues) > 1 {
			first := allValues[0]
			second := allValues[1]

			relation := first.Clock.Compare(second.Clock)
			if relation == vectorclock.Smaller {
				s.delete(key, first, 0)
			} else if relation == vectorclock.Larger {
				s.delete(key, second, 1)
			} else if first.Timestamp.After(second.Timestamp) {
				s.delete(key, second, 1)
			} else {
				s.delete(key, first, 0)
			}

			allValues, _ = s.data[key]
		}

		lock.Unlock()
	}

	atomic.StoreUint32(&s.gcInUse, 0)
}

// New creates a new instance of a KVStore.
//
// gcInterval: An interval for the garbage collection
// in milliseconds. Leaving this blank means the default
// timeout of 4 seconds.
func New(gcInterval time.Duration) *KVStore {
	newStore := KVStore{
		data:    make(map[string][]Value),
		locks:   make(map[string]*sync.RWMutex),
		gcInUse: 0,
	}

	// Garbage collection ticker.
	if gcInterval == 0 {
		gcInterval = 1000
	}
	ticker := time.NewTicker(gcInterval * time.Millisecond)

	go func(s *KVStore) {
		for range ticker.C {
			s.garbageCollection()
		}
	}(&newStore)

	return &newStore
}

// Err is returned when an error occurs.
const Err = -1

const (
	// Found is when a value was found in the KVStore.
	Found = 0

	// Old is when a value was found, but it was too old.
	Old = 1

	// Diverged is when a value was found, but it belonged to a separate context.
	Diverged = 2

	// NotFound is when no value was found in the KVStore.
	NotFound = 3
)

// Get acquires a key from the KVStore.
func (s *KVStore) Get(key string, receivedPayload []byte) (Value, []byte, int) {
	lock, exists := s.locks[key]
	if !exists {
		return Value{}, []byte{}, NotFound
	}
	lock.RLock()
	valueList, _ := s.data[key]

	var causalPayload CausalContext
	if len(receivedPayload) == 0 {
		lock.RUnlock()
		return Value{}, []byte{}, Err
	} else if err := json.Unmarshal(receivedPayload, &causalPayload); err != nil {
		lock.RUnlock()
		return Value{}, []byte{}, Err
	}

	// Find a version that is compatible. Its possible that even
	// here we may not find anything since values may not be
	// available.

	var returnValue Value
	var returnCode int
	allValues, _ := s.traverseValues(valueList, causalPayload.Clock, true)
	if len(allValues[vectorclock.Equal]) > 0 {
		returnValue = allValues[vectorclock.Equal][0]
		returnCode = Found
	} else if len(allValues[vectorclock.Larger]) > 0 {
		returnValue = allValues[vectorclock.Larger][0]
		returnCode = Found
	} else if len(allValues[vectorclock.Smaller]) > 0 {
		returnValue = allValues[vectorclock.Smaller][0]
		returnCode = Old
	} else if len(allValues[vectorclock.Uncomparable]) > 0 {
		returnValue = allValues[vectorclock.Uncomparable][0]
		returnCode = Diverged
	} else {
		lock.RUnlock()
		return Value{}, []byte{}, NotFound
	}

	lock.RUnlock()
	returnPayload, _ := json.Marshal(CausalContext{Clock: returnValue.Clock, Key: key})
	return returnValue, returnPayload, returnCode
}

const (
	// Added is returned when a new key is added.
	Added = 0

	// Replaced is returned when a key is replaced.
	Replaced = 1
)

// Put places a key-value pair into the KVStore.
func (s *KVStore) Put(key string, value Value, receivedPayload []byte, ident uint32) (Value, []byte, int) {
	var causalPayload CausalContext
	if len(receivedPayload) == 0 {
		return Value{}, []byte{}, Err
	} else if err := json.Unmarshal(receivedPayload, &causalPayload); err != nil {
		return Value{}, []byte{}, Err
	}
	value.Depends = causalPayload
	value.Clock = causalPayload.Clock.Copy()
	value.Clock.Increment(ident, 1)
	value.Available = s.isAvailable(value)

	// Acquire lock for this key.
	lock, exists := s.locks[key]
	if !exists {
		lock = &sync.RWMutex{}
		s.locks[key] = lock
	}
	lock.Lock()

	var valueList []Value
	var returnCode int
	valueList, exists = s.data[key]
	if exists {
		returnCode = Replaced
	} else {
		returnCode = Added
	}
	valueList = append(valueList, value)
	s.data[key] = valueList
	lock.Unlock()

	returnPayload, _ := json.Marshal(CausalContext{Clock: value.Clock, Key: key})
	return value, returnPayload, returnCode
}

// Delete removes all keys with an equal or older causal context
// than the one provided.
func (s *KVStore) Delete(key string, receivedPayload []byte) bool {
	var causalPayload CausalContext
	if len(receivedPayload) == 0 {
		return false
	} else if err := json.Unmarshal(receivedPayload, &causalPayload); err != nil {
		return false
	}

	// Acquire lock for this key.
	lock, exists := s.locks[key]
	if !exists {
		return false
	}
	lock.Lock()
	valueList, _ := s.data[key]

	allValues, allIndices := s.traverseValues(valueList, causalPayload.Clock, false)
	equalValues := allValues[vectorclock.Equal]
	smallerValues := allValues[vectorclock.Smaller]

	// Get rid of all equal keys.
	for i := 0; i < len(equalValues); i++ {
		s.delete(key, equalValues[i], allIndices[vectorclock.Equal][i])
	}

	// Get rid of all smaller keys.
	for i := 0; i < len(smallerValues); i++ {
		s.delete(key, smallerValues[i], allIndices[vectorclock.Smaller][i])
	}
	lock.Unlock()

	return true
}

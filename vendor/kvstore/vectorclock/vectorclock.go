// Simple API for using a vector clock.

package vectorclock

// Tuple is the building block for a vector clock.
// It is the information for a single node.
type Tuple struct {
	Identifier uint32 `json:"identifier"`
	Count      uint32 `json:"count"`
}

// VectorClock is represented as a slice of multiple
// tuples.
type VectorClock []Tuple

func (v VectorClock) Len() int {
	return len(v)
}

func (v VectorClock) Less(i, j int) bool {
	if v[i].Identifier <= v[j].Identifier {
		return true
	}
	return false
}

func (v VectorClock) Swap(i, j int) {
	temp := v[i]
	v[i] = v[j]
	v[j] = temp
}

func (v VectorClock) searchTuple(ident uint32) *Tuple {
	for i := range v {
		if v[i].Identifier == ident {
			return &v[i]
		}
	}
	return nil
}

func (v VectorClock) searchIndex(ident uint32) (uint32, bool) {
	for i := range v {
		if v[i].Identifier == ident {
			return uint32(i), true
		}
	}
	return 0, false
}

const (
	// Equal means causal history _could_ be the same.
	Equal = 0

	// Larger means current clock contains the causal history.
	Larger = 1

	// Smaller means current clock is missing causal history.
	Smaller = 2

	// Uncomparable means clocks are unrelated.
	Uncomparable = 3

	// Max Value of types.
	Max = 4
)

// New creates a new vector clock, which is iniitalized
// to zero.
func New() VectorClock {
	return make(VectorClock, 0, 4)
}

// Add adds a new node to the vector clock.
func (v VectorClock) Add(ident uint32, count uint32) (VectorClock, bool) {
	if _, exists := v.searchIndex(ident); exists {
		return v, false
	}

	v = append(v, Tuple{ident, count})
	return v, true
}

// Remove removes a node from the vector clock.
func (v VectorClock) Remove(ident uint32) (VectorClock, bool) {
	index, exists := v.searchIndex(ident)
	if exists {
		v = append(v[:index], v[index+1:]...)
	}
	return v, exists
}

// Compare is used to compare one vector clock with another.
func (v VectorClock) Compare(v2 VectorClock) int {
	hashMap1 := make(map[uint32]uint32)
	hashMap2 := make(map[uint32]uint32)

	// Add each vector clock identifiers to their
	// respective hash tables.

	for _, v := range v {
		hashMap1[v.Identifier] = v.Count
	}

	for _, v := range v2 {
		hashMap2[v.Identifier] = v.Count
	}

	// Go through each hash table and compare how
	// they match up.

	var firstLarger, secondLarger bool
	for k, val := range hashMap1 {
		val2, exists := hashMap2[k]
		if !exists {
			firstLarger = true
		} else {
			if val > val2 {
				firstLarger = true
			} else if val < val2 {
				secondLarger = true
			}
		}
	}

	for k, val2 := range hashMap2 {
		val, exists := hashMap1[k]
		if !exists {
			secondLarger = true
		} else {
			if val > val2 {
				firstLarger = true
			} else if val < val2 {
				secondLarger = true
			}
		}
	}

	if firstLarger && secondLarger {
		return Uncomparable
	} else if firstLarger {
		return Larger
	} else if secondLarger {
		return Smaller
	}
	return Equal
}

// Increment increments the vector count for a particular
// identifier.
func (v VectorClock) Increment(ident uint32, count uint32) bool {
	tuple := v.searchTuple(ident)
	if tuple == nil {
		return false
	}
	tuple.Count += count
	return true
}

// Decrement decrements the vector count for a particular
// identifier.
func (v VectorClock) Decrement(ident uint32, count uint32) bool {
	tuple := v.searchTuple(ident)
	if tuple == nil {
		return false
	}
	tuple.Count -= count
	return true
}

// ZeroClock is a boolean which tells us if this clock
// has no history.
func (v VectorClock) ZeroClock() bool {
	for _, node := range v {
		if node.Count != 0 {
			return false
		}
	}
	return true
}

// Copy is used to provide a deep-copy of the vector clock.
func (v VectorClock) Copy() VectorClock {
	var clock VectorClock

	for _, node := range v {
		clock = append(clock, Tuple{Identifier: node.Identifier, Count: node.Count})
	}

	return clock
}

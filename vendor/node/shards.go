package node

import (
	"fmt"
	"sort"
	"strconv"
)

// Shard is a collection of IP's referenced by an ID.
type Shard struct {
	View *View
	ID   uint32
}

// Shards is all the shards present in the system.
type Shards struct {
	Shards []Shard
}

// Acquire the printable string for a shard.
func (s Shard) String() string {
	str := strconv.Itoa(int(s.ID)) + ": "
	str += s.View.String()

	return str
}

// Acquire the printable string for shards.
func (s *Shards) String() string {
	str := ""

	for _, v := range s.Shards {
		str += "\n"
		str += v.String()
	}
	str += "\n"

	return str
}

// GetShard is used to return the shard which
// contains a specific IP.
func (s *Shards) GetShard(ip IP, matchPort bool) *Shard {
	for _, v := range s.Shards {
		if matchPort {
			if v.View.Exists(ip) {
				return &v
			}
		} else {
			if v.View.ExistsIP(ip) {
				return &v
			}
		}
	}

	return nil
}

// NewShards creates a number of clusters based on number provided.
// Making no shard consist of a single node is implemented through
// crappy and ugly code.
func NewShards(view *View, num int) *Shards {
	s := &Shards{Shards: make([]Shard, 0, 4)}
	v := view.GetCopy()

	if num == 0 {
		num = 1
	}
	each := len(v) / num
	extra := len(v) % num

	addList := make([]int, 0, num)
	for i := 0; i < num; i++ {
		addList = append(addList, each)
		if extra > 0 {
			addList[i]++
			extra--
		}
	}

	// Reverse traverse the list and for any index that
	// has thr value 1, move it to an index before. Don't
	// get to the first index ever!
	for i := num - 1; i >= 0; i-- {
		if i == 0 && addList[i] == 1 {
			if len(addList) > 1 {
				addList[i+1]++
				addList[i] = 0
			}
		} else if addList[i] == 1 {
			addList[i-1]++
			addList[i] = 0
		}

		if addList[i] == 0 {
			addList = append(addList[:i], addList[i+1:]...)
		}
	}

	index := 0
	for i, val := range addList {
		viewString := ""

		// Add 'each' number of IP's to the string.
		for j := 0; j < val; j++ {
			if j != 0 {
				viewString += ","
			}
			viewString += v[index].String()
			index++
		}
		shard := Shard{View: NewView(viewString, view.Self.String()), ID: uint32(i)}
		s.Shards = append(s.Shards, shard)
	}

	return s
}

// Test runs a series of unit tests for the 'node' package.
func Test() {
	self := "192.168.1.1:8080"
	view := "192.168.1.1:8080, 192.168.1.2:8080, 192.168.1.3:8080,192.168.1.4:8080"

	v := NewView(view, self)
	fmt.Println("Initial View:", v.GetCopy())

	// Add a node to the view.
	ret := v.Add(NewIP("192.168.1.5:8080"))
	fmt.Println("Ret:", ret, " {true}")
	fmt.Println("View:", v.GetCopy())

	// Add the same node again.
	ret = v.Add(NewIP("192.168.1.5:8080"))
	fmt.Println("Ret:", ret, " {false}")
	fmt.Println("View:", v.GetCopy())

	// Remove a node from the view.
	ret = v.Remove(NewIP("192.168.1.2:8080"))
	fmt.Println("Ret:", ret, " {true}")
	fmt.Println("View:", v.GetCopy())

	// Add the node back.
	ret = v.Add(NewIP("192.168.1.2:8080"))
	fmt.Println("Ret:", ret, " {true}")
	fmt.Println("View:", v.GetCopy())

	// Test for Exists and ExistsIP.
	ret = v.Exists(NewIP("192.168.1.2:8080"))
	fmt.Println("Ret:", ret, " {true}")
	ret = v.Exists(NewIP("192.168.1.2:8081"))
	fmt.Println("Ret:", ret, " {false}")
	ret = v.ExistsIP(NewIP("192.168.1.2:8081"))
	fmt.Println("Ret:", ret, " {true}")

	// Sort the view.
	sort.Sort(v)
	fmt.Println("View:", v.GetCopy())

	s := NewShards(v, 1)
	fmt.Println("Shards {1}:", s)

	s = NewShards(v, 2)
	fmt.Println("Shards {2}:", s)

	s = NewShards(v, 3)
	fmt.Println("Shards {3}:", s)

	s = NewShards(v, 5)
	fmt.Println("Shards {5}:", s)

	s = NewShards(v, 6)
	fmt.Println("Shards {6}:", s)
}

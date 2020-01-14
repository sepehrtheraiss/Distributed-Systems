package node

import (
	"strings"
	"sync"
)

// View contains the list of IP's which are part of the
// current view.
type View struct {
	Nodes []IP
	Self  IP
	lock  sync.RWMutex
}

// setSelf is used to change the Self IP.
func (v *View) setSelf(ipPort string) {
	v.lock.Lock()
	v.Self = NewIP(ipPort)
	v.lock.Unlock()
}

// Empty checks to see if there are no nodes in the view.
func (v *View) Empty() bool {
	return v.Len() == 0
}

// Len is used to acquire the number of nodes in the view.
func (v *View) Len() int {
	v.lock.RLock()
	ret := len(v.Nodes)
	v.lock.RUnlock()
	return ret
}

// Less is used to compare two items in the view.
func (v *View) Less(i, j int) bool {
	v.lock.RLock()
	relation := v.Nodes[i].Compare(v.Nodes[j])
	v.lock.RUnlock()

	return relation <= 0
}

// Swap is used to swap two items in the view.
func (v *View) Swap(i, j int) {
	v.lock.Lock()
	temp := v.Nodes[i]
	v.Nodes[i] = v.Nodes[j]
	v.Nodes[j] = temp
	v.lock.Unlock()
}

// GetCopy is used to return a copy of the view.
func (v *View) GetCopy() []IP {
	copyView := make([]IP, 0, v.Len())

	v.lock.RLock()
	for _, v := range v.Nodes {
		copyView = append(copyView, v)
	}
	v.lock.RUnlock()

	return copyView
}

// Exists checks if an IP:Port exists in in the view.
func (v *View) Exists(ip IP) bool {
	ret := false

	for _, val := range v.GetCopy() {
		if val.Compare(ip) == 0 {
			ret = true
			break
		}
	}

	return ret
}

// ExistsIP checks if an IP exists in in the view.
func (v *View) ExistsIP(ip IP) bool {
	ret := false

	for _, val := range v.GetCopy() {
		if val.IP == ip.IP {
			ret = true
			break
		}
	}

	return ret
}

// Add is used to add an IP to the view.
func (v *View) Add(ip IP) bool {
	if exists := v.Exists(ip); exists {
		return false
	}

	v.lock.Lock()
	v.Nodes = append(v.Nodes, ip)
	v.lock.Unlock()

	return true
}

// Remove is used to remove an IP from the view.
func (v *View) Remove(ip IP) bool {
	if exists := v.Exists(ip); !exists {
		return false
	}

	v.lock.Lock()
	for i, val := range v.Nodes {
		if val == ip {
			v.Nodes = append(v.Nodes[:i], v.Nodes[i+1:]...)
		}
	}
	v.lock.Unlock()

	return true
}

// StringIP is used to acquire a comma separated list of IP's.
func (v *View) StringIP() string {
	str := ""
	for i, val := range v.GetCopy() {
		if i != 0 {
			str += ","
		}
		str += val.String()
	}

	return str
}

// String is used to acquire a printable version of view.
func (v *View) String() string {
	str := "[Self: "
	str += v.Self.String() + ", ["
	str += v.StringIP()
	str += "]]"

	return str
}

// NewView creates a new View and returns a pointer to it.
func NewView(view string, self string) *View {
	v := &View{Nodes: make([]IP, 0, 4)}

	viewList := strings.Split(view, ",")
	for _, ipPort := range viewList {
		v.Add(NewIP(ipPort))
	}
	v.setSelf(self)

	return v
}

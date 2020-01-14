package node

import (
	"net/url"
	"strings"
)

// IP holds IP and port in a easily "reachable" format.
type IP struct {
	IP   string
	Port string
}

// NewIP parses a Host:IP combination and
// returns it in the IP struct format.
func NewIP(ipPort string) IP {
	ipPort = strings.TrimSpace(ipPort)
	url := url.URL{Host: ipPort}
	ip := url.Hostname()
	port := url.Port()

	return IP{ip, port}
}

// Compare is used to compare two IP's.
//  0: Equal.
// -1: i1 is smaller.
//  1: i1 is larger.
func (i IP) Compare(com IP) int {
	if i.IP == com.IP {
		if i.Port == com.Port {
			return 0
		} else if i.Port < com.Port {
			return -1
		} else {
			return 1
		}
	} else if i.IP < com.IP {
		return -1
	} else {
		return 1
	}
}

// String is used to return the IP as a Host:IP string.
func (i IP) String() string {
	return i.IP + ":" + i.Port
}

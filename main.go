// Author: Yash Gupta <ygupta@ucsc.edu>
// Author: Sepehr Raissian <sraissia@ucsc.edu>
package main

import (
	"bytes"
	"io"
	"kvstore"
	"log"
	"net/http"
	"net/url"
	"node"
	"os"
	"strconv"
	"sync"
	"time"
)

// Endpoints for the various different types of HTTP functionality
// we support.
const (
	_KVRequest = iota
	_KVSearch
	_View
	_ViewGossip
	_ShardMyID
	_ShardAllIDs
	_ShardMembers
	_ShardCount
	_ShardChangeShardNumber
)

// HTTPEndpoint is the HTTP endpoint and its context.
type HTTPEndpoint struct {
	ID      int
	Context *Context
}

// Context is the collection of variables for each endpoint.
type Context struct {
	View    *node.View
	KVStore *kvstore.KVStore

	// Shards don't need an atomic since they
	// are never part of a gossip. However, the
	// shard variable itself may change, so it
	// needs to be protected by a lock.
	Shards    *node.Shards
	shardLock sync.RWMutex
}

// MethodReply - JSON reply for an operation on the KV Store.
type MethodReply struct {
	Msg      string `json:"msg"`
	Value    string `json:"value"`
	IsExists bool   `json:"isExists"`
	Replaced bool   `json:"replaced"`
	Error    string `json:"error"`
	View     string `json:"view"`
	Result   string `json:"result"`
	Payload  string `json:"payload"`
	ID       uint32 `json:"id"`
	ShardIDs string `json:"shard_ids"`
	Members  string `json:"members"`
	Count    uint64 `json:"Count"`
	Owner    uint32 `json:"owner"`
}

func handleError(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Unsupported Endpoint", http.StatusNotFound)
}

// Send sends a message to a URL with a data stream through
// the specified HTTP Method.
func Send(url string, method string, data io.Reader, dataLength int, json bool) (*http.Response, error) {
	req, err := http.NewRequest(method, url, data)
	if err != nil {
		log.Println("could not build request for", url, ":", err)
		return nil, err
	}

	if json {
		req.Header.Set("Content-Type", "application/json")
	} else {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	req.Header.Add("Content-Length", strconv.Itoa(dataLength))

	var client = http.Client{Timeout: time.Duration(1 * time.Second)}
	r, err := client.Do(req)
	if err != nil {
		log.Println("could not send data to", url, ":", err)
	}

	return r, err
}

// Broadcast data to other nodes in the view.
func Broadcast(v *node.View, endp string, method string, data []byte, json bool) {
	for _, val := range v.GetCopy() {
		if val == v.Self {
			continue
		}
		url := url.URL{Scheme: "HTTP", Host: val.String(), Path: endp}
		Send(url.String(), method, bytes.NewReader(data), len(data), json)
	}
}

func (e HTTPEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch e.ID {
	case _KVRequest:
		handleKVRequest(e, w, r)

	case _KVSearch:
		handleKVSearch(e, w, r)

	case _View:
		handleView(e, w, r)

	case _ShardMyID:
		handleShardMyID(e, w, r)

	case _ShardAllIDs:
		handleShardAllIDs(e, w, r)

	case _ShardMembers:
		handleShardMembers(e, w, r)

	case _ShardCount:
		handleShardCount(e, w, r)

	case _ShardChangeShardNumber:
		handleShardChangeNumber(e, w, r)
	}
}

func main() {
	var context Context

	viewEnvironment := os.Getenv("VIEW")
	ipPortEnvironment := os.Getenv("IP_PORT")
	shardEnvironment, _ := strconv.Atoi(os.Getenv("S"))
	gossipEnvironment, _ := strconv.Atoi(os.Getenv("GOSSIP"))

	context.View = node.NewView(viewEnvironment, ipPortEnvironment)
	context.Shards = node.NewShards(context.View, shardEnvironment)
	context.KVStore = kvstore.New(0)

	log.Println("View Environment  : ", viewEnvironment)
	log.Println("IpPort Environment: ", ipPortEnvironment)
	log.Println("Shard Environment : ", shardEnvironment)
	log.Println("Gossip Environment: ", gossipEnvironment)
	log.Println("View:", context.View)
	log.Println("Shards", context.Shards)

	// Handle generic error.
	http.HandleFunc("/", handleError)

	// Handle view changes.
	http.Handle("/view", HTTPEndpoint{_View, &context})

	// Handle value requests on the KVStore.
	http.Handle("/keyValue-store/", HTTPEndpoint{_KVRequest, &context})
	http.Handle("/keyValue-store/search/", HTTPEndpoint{_KVSearch, &context})

	// Handle Shards.
	http.Handle("/shard/my_id", HTTPEndpoint{_ShardMyID, &context})
	http.Handle("/shard/all_ids", HTTPEndpoint{_ShardAllIDs, &context})
	http.Handle("/shard/members/", HTTPEndpoint{_ShardMembers, &context})
	http.Handle("/shard/count/", HTTPEndpoint{_ShardCount, &context})
	http.Handle("/shard/changeShardNumber", HTTPEndpoint{_ShardChangeShardNumber, &context})

	// Start the HTTP Server.
	go func(c *Context) {
		log.Fatal(http.ListenAndServe(c.View.Self.String(), nil))
	}(&context)

	// Anti-Entropy.
	if gossipEnvironment == 0 {
		gossipEnvironment = 4
	}

	ticker := time.NewTicker(time.Duration(gossipEnvironment) * time.Second)
	go func(c *Context) {
		for range ticker.C {
			gossipView(c)
		}
	}(&context)

	// Don't exit the main routine.
	select {}
}

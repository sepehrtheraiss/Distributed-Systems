package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"node"
	"strconv"
	"strings"
)

// ShardTransmit is for the number of shards to change
type ShardTransmit struct {
	Num string `json:"num"`
}

// handleShardMyID is the HTTP endpoint for handling /shard/my_id.
func handleShardMyID(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	var jsonReply []byte
	var statusCode int

	switch r.Method {
	case "GET":
		e.Context.shardLock.RLock()
		id := e.Context.Shards.GetShard(e.Context.View.Self, true).ID
		e.Context.shardLock.RUnlock()

		jsonReply, _ = json.Marshal(MethodReply{ID: id})
		statusCode = http.StatusOK

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

// handleShardAllIDs is the HTTP endpoint for handling /shard/all_ids.
func handleShardAllIDs(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	var jsonReply []byte
	var statusCode int

	switch r.Method {
	case "GET":
		e.Context.shardLock.RLock()
		len := len(e.Context.Shards.Shards)
		e.Context.shardLock.RUnlock()

		reply := ""
		for i := 0; i < len; i++ {
			if i != 0 {
				reply += ","
			}
			reply += strconv.Itoa(i)
		}

		jsonReply, _ = json.Marshal(MethodReply{ShardIDs: reply})
		statusCode = http.StatusOK

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

// handleShardMembers is the HTTP endpoint for handling /shard/members/.
func handleShardMembers(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	shardID, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/shard/members/"))
	var jsonReply []byte
	var statusCode int

	switch r.Method {
	case "GET":
		e.Context.shardLock.RLock()
		len := len(e.Context.Shards.Shards)
		e.Context.shardLock.RUnlock()

		if shardID >= len {
			msg := "No shard with id " + strconv.Itoa(shardID)
			jsonReply, _ = json.Marshal(MethodReply{Result: "Error", Msg: msg})
			statusCode = http.StatusNotFound
			break
		}

		e.Context.shardLock.RLock()
		shard := e.Context.Shards.Shards[shardID]
		reply := shard.View.StringIP()
		e.Context.shardLock.RUnlock()

		jsonReply, _ = json.Marshal(MethodReply{Members: reply})
		statusCode = http.StatusOK

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return

	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

// handleShardCount is the HTTP endpoint for handling /shard/count/.
func handleShardCount(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	shardID, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/shard/count/"))
	var jsonReply []byte
	var statusCode int

	switch r.Method {
	case "GET":
		e.Context.shardLock.RLock()
		len := len(e.Context.Shards.Shards)
		e.Context.shardLock.RUnlock()

		if shardID >= len {
			msg := "No shard with id " + strconv.Itoa(shardID)
			jsonReply, _ = json.Marshal(MethodReply{Result: "Error", Msg: msg})
			statusCode = http.StatusNotFound
			break
		}

		reply := uint64(math.MaxUint32 / len)
		jsonReply, _ = json.Marshal(MethodReply{Count: reply})
		statusCode = http.StatusOK

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

// handleShardChangeNumber is the HTTP endpoint for handling /shard/changeShardNumber.
func handleShardChangeNumber(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	var jsonReply []byte
	var statusCode int

	r.ParseForm()

	switch r.Method {
	case "PUT":
		nodes := e.Context.View.Len()
		newShardsStr := r.FormValue("num")
		newShards, _ := strconv.Atoi(newShardsStr)

		if newShards > nodes {
			msg := "Not enough nodes for " + newShardsStr + "shards"
			jsonReply, _ = json.Marshal(MethodReply{Result: "Error", Msg: msg})
			statusCode = http.StatusBadRequest
			break
		}

		// TODO: Handle case where a shard may contain only one node.

		e.Context.shardLock.Lock()
		e.Context.Shards = node.NewShards(e.Context.View, newShards)
		e.Context.shardLock.Unlock()

		// Broadcast to everyone if required.
		connectionIP := node.NewIP(r.RemoteAddr)
		nodeConnecion := e.Context.View.ExistsIP(connectionIP)
		if !nodeConnecion {
			Broadcast(e.Context.View, "/shard/changeShardNumber", "PUT", []byte("num="+newShardsStr), false)
		}

		// This method should return everything the same as handleShardAllIDs for success.
		r.Method = "GET"
		handleShardAllIDs(e, w, r)
		return

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

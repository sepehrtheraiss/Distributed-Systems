package main

import (
	"encoding/json"
	"log"
	"net/http"
	"node"
)

// handleView is the HTTP endpoint for handling /view.
func handleView(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	var jsonReply []byte
	var statusCode int

	// Ugly hack: Apparently, golang does not parse the body
	// of the request if the METHOD is not PUT or POST.
	method := r.Method
	r.Method = "PUT"
	r.ParseForm()
	r.Method = method

	switch r.Method {
	case "GET":
		view := e.Context.View.StringIP()

		jsonReply, _ = json.Marshal(MethodReply{View: view})
		statusCode = http.StatusOK

	case "PUT":
		ipPort := r.FormValue("ip_port")
		ip := node.NewIP(ipPort)

		exists := e.Context.View.Exists(ip)
		if exists {
			msg := ipPort + " is already in view"
			jsonReply, _ = json.Marshal(MethodReply{Result: "Error", Msg: msg})
			statusCode = http.StatusNotFound
			break
		}

		// Add new IP to the view.
		e.Context.View.Add(ip)

		// Initiate a Shard change.
		e.Context.shardLock.Lock()
		e.Context.Shards = node.NewShards(e.Context.View, len(e.Context.Shards.Shards))
		e.Context.shardLock.Unlock()

		// TODO: Initiate KV Redistribution.

		// Broadcast to everyone if required.
		connectionIP := node.NewIP(r.RemoteAddr)
		nodeConnecion := e.Context.View.ExistsIP(connectionIP)
		if !nodeConnecion {
			Broadcast(e.Context.View, "/view", "PUT", []byte("ip_port="+ip.String()), false)
		}

		msg := "Successfully added " + ipPort + " to view"
		jsonReply, _ = json.Marshal(MethodReply{Result: "Success", Msg: msg})
		statusCode = http.StatusOK

	case "DELETE":
		ipPort := r.FormValue("ip_port")
		ip := node.NewIP(ipPort)

		exists := e.Context.View.Exists(ip)
		if !exists {
			msg := ipPort + " is not in current view"
			jsonReply, _ = json.Marshal(MethodReply{Result: "Error", Msg: msg})
			statusCode = http.StatusNotFound
			break
		}

		// Remove IP from the view.
		e.Context.View.Remove(ip)

		// Initiate a Shard change.
		e.Context.shardLock.Lock()
		e.Context.Shards = node.NewShards(e.Context.View, len(e.Context.Shards.Shards))
		e.Context.shardLock.Unlock()

		// TODO: Initiate KV Redistribution.

		// Broadcast to everyone if required.
		connectionIP := node.NewIP(r.RemoteAddr)
		nodeConnecion := e.Context.View.ExistsIP(connectionIP)
		if !nodeConnecion {
			Broadcast(e.Context.View, "/view", "DELETE", []byte("ip_port="+ip.String()), false)
		}

		msg := "Successfully removed " + ipPort + " from view"
		jsonReply, _ = json.Marshal(MethodReply{Result: "Success", Msg: msg})
		statusCode = http.StatusOK

	default:
		log.Println(http.StatusMethodNotAllowed, "Only GET, PUT, DELETE methods allowed.")
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

// gossipView is woken up at intervals, and each node basically
// broadcasts its view to all others in the same cluster.
//
// It is entirely possible that nodes which have been deleted
// from the view may show up again because of race conditions.
func gossipView(c *Context) {
	c.shardLock.RLock()
	myShard := c.Shards.GetShard(c.View.Self, true)

	for _, v := range c.View.GetCopy() {
		Broadcast(myShard.View, "/view", "PUT", []byte("ip_port="+v.String()), false)
	}
	c.shardLock.RUnlock()
}

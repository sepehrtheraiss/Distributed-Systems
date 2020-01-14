package main

import (
	"encoding/json"
	"io/ioutil"
	"kvstore"
	"kvstore/vectorclock"
	"log"
	"net/http"
	"node"
	"strings"
	"time"
)

// ReplicaTransmit is the struct encoded as JSON sent to
// replicas for broadcasts.
type ReplicaTransmit struct {
	Value   kvstore.Value `json:"val"`
	Payload string        `json:"payload"`
}

// This function (cheaply) just calls the getKVRequest function. If the return
// code is http.StatusOK, then we say that the key exists.
func searchKVRequest(s *kvstore.KVStore, key string, payload string) ([]byte, int) {
	jsonReply, retCode := getKVRequest(s, key, payload)

	// We need to get back the payload embedded within jsonReply.
	var replyInformation MethodReply
	_ = json.Unmarshal(jsonReply, &replyInformation)

	if retCode == http.StatusOK {
		jsonReply, _ = json.Marshal(MethodReply{Owner: 0, IsExists: true, Result: "Success", Payload: replyInformation.Payload})
	} else {
		jsonReply, _ = json.Marshal(MethodReply{IsExists: false, Result: "Success", Payload: replyInformation.Payload})
	}

	return jsonReply, http.StatusOK
}

func getKVRequest(s *kvstore.KVStore, key string, payload string) ([]byte, int) {
	if payload == "" {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Key does not exist", Payload: payload})
		return jsonReply, http.StatusNotFound
	}
	receivedPayload := []byte(payload)

	value, retPayload, retCode := s.Get(key, receivedPayload)
	if retCode == kvstore.Err {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Something fucked up!", Payload: payload})
		return jsonReply, http.StatusForbidden
	} else if retCode == kvstore.NotFound {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Key does not exist", Payload: payload})
		return jsonReply, http.StatusNotFound
	} else if retCode == kvstore.Diverged {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Value is not causally related", Payload: payload})
		return jsonReply, http.StatusConflict
	} else if retCode == kvstore.NotFound {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Unable to serve request and maintain causal consistency", Payload: payload})
		return jsonReply, http.StatusBadRequest
	}

	jsonReply, _ := json.Marshal(MethodReply{Owner: 0, Result: "Success", Value: value.Data, Payload: string(retPayload)})
	return jsonReply, http.StatusOK
}

func putKVRequest(s *kvstore.KVStore, key string, data string, payload string, ident uint32, nodeConnecion bool) (kvstore.Value, []byte, int) {
	if payload == "" {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Something fucked up!", Payload: payload})
		return kvstore.Value{}, jsonReply, http.StatusUnauthorized
	}
	receivedPayload := []byte(payload)

	// Confirm for correct data.
	if data == "" {
		jsonReply, _ := json.Marshal(MethodReply{Msg: "Error", Error: "Value is missing"})
		return kvstore.Value{}, jsonReply, http.StatusUnprocessableEntity
	} else if len(data) > (1024 * 1024) {
		jsonReply, _ := json.Marshal(MethodReply{Msg: "Error", Error: "Object too large. Size limit is 1MB"})
		return kvstore.Value{}, jsonReply, http.StatusUnprocessableEntity
	}

	var dataValue string
	var timestamp time.Time

	// If nodeConnection is true, it means we received a request from a node
	// instead of a client. Hence, the data is of type kvstore.Value.
	if nodeConnecion {
		var value kvstore.Value
		_ = json.Unmarshal([]byte(data), &value)

		dataValue = value.Data
		timestamp = value.Timestamp
	} else {
		dataValue = data
		timestamp = time.Now()
	}

	value, retPayload, retCode := s.Put(key, kvstore.Value{Data: dataValue, Timestamp: timestamp}, receivedPayload, ident)
	if retCode == kvstore.Err {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Something fucked up!", Payload: payload})
		return kvstore.Value{}, jsonReply, http.StatusForbidden
	} else if retCode == kvstore.Added {
		jsonReply, _ := json.Marshal(MethodReply{Replaced: false, Msg: "Added successfully", Payload: string(retPayload)})
		return value, jsonReply, http.StatusOK
	} else {
		jsonReply, _ := json.Marshal(MethodReply{Replaced: true, Msg: "Updated successfully", Payload: string(retPayload)})
		return value, jsonReply, http.StatusCreated
	}
}

func deleteKVRequest(s *kvstore.KVStore, key string, payload string) ([]byte, int) {
	if payload == "" {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Key does not exist", Payload: payload})
		return jsonReply, http.StatusNotFound
	}
	receivedPayload := []byte(payload)

	removed := s.Delete(key, receivedPayload)
	if !removed {
		jsonReply, _ := json.Marshal(MethodReply{Result: "Error", Msg: "Key does not exist", Payload: payload})
		return jsonReply, http.StatusNotFound
	}

	jsonReply, _ := json.Marshal(MethodReply{Result: "Success", Msg: "Key deleted", Payload: payload})
	return jsonReply, http.StatusOK
}

// We need to remove the "" the json encoder adds in their
// crappy ass test script.
func cleanPayload(payload string) string {
	if len(payload) < 2 {
		return payload
	}

	if string(payload[0]) == "\"" {
		payload = payload[1:]
	}

	if string(payload[len(payload)-1]) == "\"" {
		payload = payload[0 : len(payload)-1]
	}

	// Remove all \ symbols from behind quotations.
	split := strings.Split(payload, "\\\"")
	payload = ""
	for i, v := range split {
		if i == 0 {
			payload = payload + v
		} else {
			payload = payload + "\"" + v
		}
	}

	return payload
}

// handleKVRequest is the HTTP endpoint for handling /keyValue-store/.
func handleKVRequest(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL, r.RemoteAddr)

	var jsonReply []byte
	var statusCode int
	key := strings.TrimPrefix(r.URL.Path, "/keyValue-store/")
	store := e.Context.KVStore

	w.Header().Set("Content-Type", "application/json")
	if len(key) < 1 || len(key) > 200 {
		jsonReply, _ = json.Marshal(MethodReply{Msg: "Error", Error: "Key not valid"})
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write(jsonReply)
		return
	}

	// Ugly hack: Apparently, golang does not parse the body
	// of the request if the METHOD is not PUT or POST.
	method := r.Method
	r.Method = "PUT"
	r.ParseForm()
	r.Method = method

	switch r.Method {
	case "GET":
		payload := cleanPayload(r.FormValue("payload"))
		log.Println("Get Payload:", payload)

		jsonReply, statusCode = getKVRequest(store, key, payload)

	case "DELETE":
		var payload string
		connectionIP := node.NewIP(r.RemoteAddr)

		e.Context.shardLock.RLock()
		nodeConnecion := e.Context.Shards.GetShard(connectionIP, false)
		e.Context.shardLock.RUnlock()

		if nodeConnecion == nil {
			payload = cleanPayload(r.FormValue("payload"))
			log.Println("Delete Payload:", payload)
		} else {
			var replicaData ReplicaTransmit
			bodyData, _ := ioutil.ReadAll(r.Body)
			_ = json.Unmarshal(bodyData, &replicaData)

			payload = replicaData.Payload
		}

		jsonReply, statusCode = deleteKVRequest(store, key, payload)

		if nodeConnecion == nil {
			// TODO: Broadcast only to shard view.
			jsonTransmit, _ := json.Marshal(ReplicaTransmit{Payload: payload})
			Broadcast(e.Context.View, r.URL.Path, "DELETE", jsonTransmit, true)
		}

	case "PUT":
		var ident uint32
		var data string
		var payload string
		var value kvstore.Value

		connectionIP := node.NewIP(r.RemoteAddr)

		e.Context.shardLock.RLock()
		nodeConnection := e.Context.Shards.GetShard(connectionIP, false)
		e.Context.shardLock.RUnlock()

		// TODO: Potentially put under lock.
		if nodeConnection == nil {
			e.Context.shardLock.RLock()
			ident = e.Context.Shards.GetShard(e.Context.View.Self, true).ID
			e.Context.shardLock.RUnlock()

			data = r.FormValue("val")
			payload = cleanPayload(r.FormValue("payload"))
		} else {
			var replicaData ReplicaTransmit
			bodyData, _ := ioutil.ReadAll(r.Body)
			_ = json.Unmarshal(bodyData, &replicaData)

			byteData, _ := json.Marshal(replicaData.Value)
			data = string(byteData)
			payload = replicaData.Payload
			ident = nodeConnection.ID
		}
		log.Println("Node Connection:", nodeConnection)
		log.Println("Data:", data)
		log.Println("Put Payload:", payload)

		if payload == "" {
			var emptyClock vectorclock.VectorClock

			e.Context.shardLock.RLock()
			for _, v := range e.Context.Shards.Shards {
				emptyClock, _ = emptyClock.Add(v.ID, 0)
			}
			e.Context.shardLock.RUnlock()

			jsonString, _ := json.Marshal(kvstore.CausalContext{Clock: emptyClock, Key: ""})
			payload = string(jsonString)
		}
		value, jsonReply, statusCode = putKVRequest(store, key, data, payload, ident, nodeConnection != nil)
		log.Println("Value:", value)

		if nodeConnection == nil {
			// TODO: Broadcast only to shard view.
			jsonTransmit, _ := json.Marshal(ReplicaTransmit{Value: value, Payload: payload})
			Broadcast(e.Context.View, r.URL.Path, "PUT", jsonTransmit, true)
		}

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

// handleKVSearch is the HTTP endpoint for handling /keyValue-store/search/.
func handleKVSearch(e HTTPEndpoint, w http.ResponseWriter, r *http.Request) {
	log.Println(r.Method, r.URL)

	var jsonReply []byte
	var statusCode int
	key := strings.TrimPrefix(r.URL.Path, "/keyValue-store/search/")
	store := e.Context.KVStore

	w.Header().Set("Content-Type", "application/json")
	if len(key) < 1 || len(key) > 200 {
		jsonReply, _ = json.Marshal(MethodReply{Msg: "Error", Error: "Key not valid"})
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write(jsonReply)
		return
	}

	// Ugly hack: Apparently, golang does not parse the body
	// of the request if the METHOD is not PUT or POST.
	method := r.Method
	r.Method = "PUT"
	r.ParseForm()
	r.Method = method

	switch r.Method {
	case "GET":
		payload := cleanPayload(r.FormValue("payload"))
		log.Println("Search Get Payload:", payload)

		jsonReply, statusCode = searchKVRequest(store, key, payload)

	default:
		http.Error(w, "Unsupported Method for Endpoint", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(statusCode)
	w.Write(jsonReply)
}

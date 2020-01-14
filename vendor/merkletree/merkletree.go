package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"
)

// Data is the interface a data type needs to implement
// in order to be a compatible type for a Merkle Tree.
type Data interface {
	hash() [sha256.Size]byte
}

// Node is a node of the Merkle Tree.
type Node struct {
	Leaf  bool
	Hash  [sha256.Size]byte
	Left  *Node
	Right *Node
	Data  Data
	level int
}

// Build creates the tree based on the data passed and returns
// the root of the Merkle Tree.
func Build(data []Data) *Node {
	nodeList := make([]Node, 0, len(data))

	if len(data) == 0 {
		return nil
	}

	// Build all leaf nodes.
	for _, v := range data {
		node := Node{Leaf: true, Hash: v.hash(), Data: v, level: 0}
		nodeList = append(nodeList, node)
	}

	// Build the tree.
	for len(nodeList) > 1 {
		first := nodeList[0]
		second := nodeList[1]

		// If only a single element is present on this level,
		// duplicate it. This should only happen on the 0th level.
		if first.level != second.level {
			second = first
			nodeList = nodeList[1:]
		} else {
			nodeList = nodeList[2:]
		}

		parentHash := sha256.New()
		parentHash.Write(first.Hash[:])
		parentHash.Write(second.Hash[:])

		parentNode := Node{Leaf: false, Left: &first, Right: &second, level: first.level + 1}
		copy(parentNode.Hash[:], parentHash.Sum(nil)[0:sha256.Size])

		nodeList = append(nodeList, parentNode)
	}

	// The first and the only element is the root.
	return &nodeList[0]
}

type testData struct {
	data uint32
}

func (d testData) hash() [sha256.Size]byte {
	byteStream := make([]byte, 4)
	binary.LittleEndian.PutUint32(byteStream, d.data)

	return sha256.Sum256(byteStream)
}

func main() {
	var data [5]Data

	rand.Seed(time.Now().UTC().UnixNano())
	data[0] = testData{data: rand.Uint32()}
	data[1] = testData{data: rand.Uint32()}
	data[2] = testData{data: rand.Uint32()}
	data[3] = testData{data: rand.Uint32()}
	data[4] = testData{data: rand.Uint32()}

	root := Build(data[:])
	fmt.Println("Hash:", hex.EncodeToString(root.Hash[:]))
}

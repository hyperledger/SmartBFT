// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

const (
	incBuffSize = 1000
)

type handler interface {
	HandleMessage(sender uint64, m *smartbftprotos.Message)
	HandleRequest(sender uint64, req []byte)
	Stop()
}

type msgFrom struct {
	message proto.Message
	from    int
}

// Network is a map of ids and nodes
type Network struct {
	nodes map[uint64]*Node
	lock  sync.RWMutex
}

func NewNetwork() *Network {
	return &Network{
		nodes: make(map[uint64]*Node),
	}
}

// AddOrUpdateNode adds or updates a node in the network
func (n *Network) AddOrUpdateNode(id uint64, h handler, app *App) {
	n.lock.RLock()
	node, exists := n.nodes[id]
	if exists {
		node.h = h
		n.lock.RUnlock()
		return
	}
	n.lock.RUnlock()

	node = &Node{
		in:                  make(chan msgFrom, incBuffSize),
		h:                   h,
		shutdownChan:        make(chan struct{}),
		n:                   n,
		id:                  id,
		peerLossProbability: make(map[uint64]float32),
		peerMutatingFunc:    make(map[uint64]func(uint64, *smartbftprotos.Message)),
		app:                 app,
	}
	n.lock.Lock()
	n.nodes[id] = node
	n.lock.Unlock()

	node.createCommittedBatches(n)
}

// StartServe calls serve on all nodes in the network
func (n *Network) StartServe() {
	n.lock.RLock()
	defer n.lock.RUnlock()
	for _, node := range n.nodes {
		node.running.Add(1)
		go node.serve()
	}
}

// StopServe stops serve for all nodes in the network
func (n *Network) StopServe() {
	n.lock.RLock()
	defer n.lock.RUnlock()
	for _, node := range n.nodes {
		close(node.shutdownChan)
		node.running.Wait()
		node.shutdownChan = make(chan struct{}) // reopen for next time
	}
}

// Shutdown stops all nodes in the network
func (n *Network) Shutdown() {
	n.lock.RLock()
	defer n.lock.RUnlock()
	for _, node := range n.nodes {
		close(node.shutdownChan)
		node.running.Wait()
	}
	for _, node := range n.nodes {
		node.h.Stop()
	}
}

func (n *Network) send(source, target uint64, msg proto.Message) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	dstNode, found := n.nodes[target]

	if !found {
		panic("node doesn't exist")
	}

	srcNode, found := n.nodes[source]
	if !found {
		panic("node doesn't exist")
	}

	dstNode.probabilityLock.RLock()
	p := dstNode.lossProbability
	dstNode.probabilityLock.RUnlock()

	srcNode.probabilityLock.RLock()
	q := srcNode.lossProbability
	w := srcNode.peerLossProbability[target]
	srcNode.probabilityLock.RUnlock()

	r := rand.Float32()
	if r < p || r < q || r < w {
		return
	}

	select {
	case dstNode.in <- msgFrom{from: int(source), message: msg}:
	default:
		fmt.Println("Dropped msg from", source, "to", target, "due to overflow")
	}
}

func (n *Network) getAll() []uint64 {
	n.lock.RLock()
	defer n.lock.RUnlock()
	res := make([]uint64, 0, len(n.nodes))
	for _, i := range n.nodes {
		res = append(res, i.id)
	}

	return res
}

func (n *Network) Nodes() []*Node {
	n.lock.RLock()
	defer n.lock.RUnlock()
	res := make([]*Node, 0, len(n.nodes))
	for _, i := range n.nodes {
		res = append(res, i)
	}

	return res
}

func (n *Network) Count() int {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return len(n.nodes)
}

func (n *Network) GetByID(id uint64) *Node {
	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.nodes[id]
}

// Node represents a node in a network
type Node struct {
	sync.RWMutex
	running             sync.WaitGroup
	id                  uint64
	n                   *Network
	lossProbability     float32
	peerLossProbability map[uint64]float32
	syncDelay           <-chan struct{}
	probabilityLock     sync.RWMutex
	peerMutatingFunc    map[uint64]func(uint64, *smartbftprotos.Message)
	mutatingFuncLock    sync.RWMutex
	shutdownChan        chan struct{}
	in                  chan msgFrom
	h                   handler
	cb                  *committedBatches
	app                 *App
}

// SendConsensus sends a consensus related message to a target node
func (node *Node) SendConsensus(targetID uint64, m *smartbftprotos.Message) {
	node.mutatingFuncLock.RLock()
	mutatingFunc := node.peerMutatingFunc[targetID]
	msg := m
	if mutatingFunc != nil {
		msg = proto.Clone(m).(*smartbftprotos.Message)
		mutatingFunc(targetID, msg)
	}
	node.mutatingFuncLock.RUnlock()
	node.n.send(node.id, targetID, msg)
}

// SendTransaction sends a client's request to a target node
func (node *Node) SendTransaction(targetID uint64, request []byte) {
	node.n.send(node.id, targetID, &FwdMessage{Payload: request})
}

// Nodes returns the ids of all nodes in the network
func (node *Node) Nodes() []uint64 {
	res := node.n.getAll()
	slices.Sort(res)
	return res
}

func (node *Node) serve() {
	defer node.running.Done()
	for {
		select {
		case <-node.shutdownChan:
			return
		case m := <-node.in:
			node.RLock()
			handler := node.h
			node.RUnlock()
			switch msg := m.message.(type) {
			case *smartbftprotos.Message:
				if node.app != nil && node.app.messageLost != nil && node.app.messageLost(msg) {
					continue
				}
				handler.HandleMessage(uint64(m.from), msg)
			default:
				handler.HandleRequest(uint64(m.from), msg.(*FwdMessage).GetPayload())
			}
		}
	}
}

func (node *Node) createCommittedBatches(network *Network) {
	ns := network.Nodes()
	for _, n := range ns {
		if n.cb != nil {
			node.cb = n.cb
			return
		}
	}
	node.cb = &committedBatches{}
}

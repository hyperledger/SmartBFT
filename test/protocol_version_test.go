// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
)

// TestMessagesCarryProtocolVersion makes sure that every consensus message a node sends carries its protocol version,
// whichever component sends it: the views, the heartbeat monitor, and the view changer, which a leader failure brings in.
func TestMessagesCarryProtocolVersion(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		nodes = append(nodes, newNode(uint64(i), network, t.Name(), testDir, false, 0))
	}

	var lock sync.Mutex
	received := map[string]int{} // the number of messages received of each type
	var unversioned []string
	for _, n := range nodes {
		n.LoseMessages(func(m *smartbftprotos.Message) bool {
			lock.Lock()
			defer lock.Unlock()
			received[messageType(m)]++
			if m.GetVersion() != types.ProtocolVersion {
				unversioned = append(unversioned, bft.MsgToString(m))
			}
			return false // only observe the message
		})
	}

	// a faster clock, so that the leader also sends heartbeats while it has nothing to propose
	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.Setup()
	}
	done := make(chan struct{})
	defer close(done)
	var counter uint64
	accelerateTime(nodes, done, true, false, &counter)

	startNodes(nodes, network)
	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	for i := range numberOfNodes {
		delivered(t, nodes[i])
	}
	waitForHeartbeat := time.After(time.Minute)
	for {
		lock.Lock()
		heartbeats := received["heart_beat"]
		lock.Unlock()
		if heartbeats > 0 {
			break
		}
		select {
		case <-waitForHeartbeat:
			t.Fatal("no heartbeat was received")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// the leader goes away, so the others change the view to decide another request
	nodes[0].Disconnect()
	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "2", ClientID: "bob"})
	}
	for i := 1; i < numberOfNodes; i++ {
		delivered(t, nodes[i])
	}

	lock.Lock()
	defer lock.Unlock()
	for _, messageType := range []string{"pre_prepare", "prepare", "commit", "heart_beat", "view_change", "view_data", "new_view"} {
		require.NotZero(t, received[messageType], "no %s message was received", messageType)
	}
	require.Empty(t, unversioned, "messages were sent without protocol version %d", types.ProtocolVersion)
}

// TestMixedProtocolVersions makes sure that nodes keep working together with nodes of an earlier release,
// which send messages without a version, and ignore the messages of a node with a later protocol version.
// The other nodes are simulated by setting the version of every message they send.
func TestMixedProtocolVersions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		description string
		version     uint64   // the protocol version the other nodes send
		others      []uint64 // the other nodes
	}{
		{description: "a follower of an earlier release", version: 0, others: []uint64{4}},
		{description: "the leader of an earlier release", version: 0, others: []uint64{1}},
		{description: "half of the nodes of an earlier release", version: 0, others: []uint64{1, 2}},
		{description: "a follower of a later protocol version", version: types.ProtocolVersion + 1, others: []uint64{4}},
		{description: "the leader of a later protocol version", version: types.ProtocolVersion + 1, others: []uint64{1}},
	} {
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()
			network := NewNetwork()
			defer network.Shutdown()

			testDir := t.TempDir()

			numberOfNodes := 4
			nodes := make([]*App, 0, numberOfNodes)
			for i := 1; i <= numberOfNodes; i++ {
				nodes = append(nodes, newNode(uint64(i), network, t.Name(), testDir, false, 0))
			}
			for _, id := range test.others {
				for target := uint64(1); target <= uint64(numberOfNodes); target++ {
					nodes[id-1].MutateSend(target, func(_ uint64, m *smartbftprotos.Message) {
						m.Version = test.version
					})
				}
			}

			startNodes(nodes, network)

			if !types.SupportedProtocolVersion(test.version) {
				// the messages of the other node are ignored, so the remaining nodes decide without it,
				// after a view change if it is the leader
				var remaining []*App
				for _, n := range nodes {
					if !slices.Contains(test.others, n.ID) {
						remaining = append(remaining, n)
					}
				}
				for _, n := range remaining {
					n.Submit(Request{ID: "1", ClientID: "alice"})
				}
				data := make([]*AppRecord, 0, len(remaining))
				for _, n := range remaining {
					data = append(data, delivered(t, n))
				}
				for i := range len(data) - 1 {
					require.Equal(t, data[i], data[i+1])
				}
				return
			}

			// requests are decided, and every node delivers the same blocks
			for i := 1; i <= 3; i++ {
				nodes[0].Submit(Request{ID: strconv.Itoa(i), ClientID: "alice"})
			}
			data := make([]*AppRecord, 0, numberOfNodes)
			for i := range numberOfNodes {
				data = append(data, delivered(t, nodes[i]))
			}
			for i := range numberOfNodes - 1 {
				require.Equal(t, data[i], data[i+1])
			}

			// a view change completes too: the leader goes away, and the others decide a new request
			nodes[0].Disconnect()
			for i := 1; i < numberOfNodes; i++ {
				nodes[i].Submit(Request{ID: "4", ClientID: "bob"})
			}
			data = data[:0]
			for i := 1; i < numberOfNodes; i++ {
				data = append(data, delivered(t, nodes[i]))
			}
			for i := range len(data) - 1 {
				require.Equal(t, data[i], data[i+1])
			}
		})
	}
}

// delivered returns the next decision the node delivers, and fails the test if it doesn't deliver one in time.
func delivered(t *testing.T, node *App) *AppRecord {
	t.Helper()
	select {
	case d := <-node.Delivered:
		return d
	case <-time.After(time.Minute):
		t.Fatalf("node %d did not deliver a decision", node.ID)
		return nil
	}
}

// messageType returns the name of the type of the message, for example "prepare".
func messageType(m *smartbftprotos.Message) string {
	content := m.ProtoReflect().WhichOneof(m.ProtoReflect().Descriptor().Oneofs().ByName("content"))
	if content == nil {
		return "none"
	}
	return string(content.Name())
}

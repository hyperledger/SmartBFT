// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

// TestMixedProtocolVersions makes sure that nodes ignore the messages of nodes of another protocol version,
// whether of an earlier release, which send messages without a version or with version 1, or of a later one.
// The other nodes are simulated by setting the version of every message they send. A simulated node of version 1 also
// ignores the messages of later versions it receives, as v1.0.3 does. A real node without a version would process them
// with the old protocol, which the harness can't run, so those cases only check that the other nodes ignore it.
func TestMixedProtocolVersions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		description string
		version     uint64   // the protocol version the other nodes send
		others      []uint64 // the other nodes
	}{
		{description: "a follower without a version", version: 0, others: []uint64{4}},
		{description: "the leader without a version", version: 0, others: []uint64{1}},
		{description: "a follower of version 1", version: 1, others: []uint64{4}},
		{description: "the leader of version 1", version: 1, others: []uint64{1}},
		{description: "a follower of a later protocol version", version: types.ProtocolVersion + 1, others: []uint64{4}},
		{description: "the leader of a later protocol version", version: types.ProtocolVersion + 1, others: []uint64{1}},
		{description: "half of the nodes without a version, with the leader", version: 0, others: []uint64{1, 2}},
		{description: "half of the nodes without a version, without the leader", version: 0, others: []uint64{3, 4}},
		{description: "half of the nodes of version 1, with the leader", version: 1, others: []uint64{1, 2}},
		{description: "half of the nodes of version 1, without the leader", version: 1, others: []uint64{3, 4}},
	} {
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()
			require.False(t, types.SupportedProtocolVersion(test.version))

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
				if test.version == 1 {
					nodes[id-1].LoseMessages(func(m *smartbftprotos.Message) bool { return m.GetVersion() > 1 })
				}
			}

			startNodes(nodes, network)

			var remaining []*App
			for _, n := range nodes {
				if !slices.Contains(test.others, n.ID) {
					remaining = append(remaining, n)
				}
			}
			for _, n := range nodes {
				n.Submit(Request{ID: "1", ClientID: "alice"})
			}

			if len(remaining) < 3 { // fewer than a quorum on either side
				// neither side decides, through the complaints and the view change attempts that follow
				timeout := time.After(5 * nodes[0].Consensus.Config.RequestComplainTimeout)
				for {
					select {
					case <-timeout:
						return
					case <-nodes[0].Delivered:
					case <-nodes[1].Delivered:
					case <-nodes[2].Delivered:
					case <-nodes[3].Delivered:
					}
					t.Fatal("a node delivered a decision without a quorum of its protocol version")
				}
			}

			// the messages of the other node are ignored, so the remaining nodes decide without it,
			// after a view change if it is the leader
			data := make([]*AppRecord, 0, len(remaining))
			for _, n := range remaining {
				data = append(data, delivered(t, n))
			}
			for i := range len(data) - 1 {
				require.Equal(t, data[i], data[i+1])
			}
			if slices.Contains(test.others, 1) {
				// the leader of view 0 was ignored, so the decision was made in a later view
				md := &smartbftprotos.ViewMetadata{}
				require.NoError(t, proto.Unmarshal(data[0].Metadata, md))
				require.NotZero(t, md.GetViewId(), "the remaining nodes decided on a proposal of the ignored leader")
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

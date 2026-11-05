// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"sync"
	"testing"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
)

// TestMessagesCarryProtocolVersion makes sure that every consensus message a node sends carries its protocol version,
// whichever component sends it.
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
	received := 0
	var unversioned []string
	for _, n := range nodes {
		n.LoseMessages(func(m *smartbftprotos.Message) bool {
			lock.Lock()
			defer lock.Unlock()
			received++
			if m.GetVersion() != bft.ProtocolVersion {
				unversioned = append(unversioned, bft.MsgToString(m))
			}
			return false // only observe the message
		})
	}

	startNodes(nodes, network)
	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	for i := range numberOfNodes {
		<-nodes[i].Delivered
	}

	lock.Lock()
	defer lock.Unlock()
	require.NotZero(t, received)
	require.Empty(t, unversioned, "messages were sent without protocol version %d", bft.ProtocolVersion)
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNodeJoinsViewChangeOfFPlusOneNodes makes sure that a correct node that gets view change messages from only f+1
// nodes is not left behind in a view the other correct nodes have left.
// With 7 nodes f is 2 and the quorum is 5. The leader goes away, and nodes 2 to 6 change the view without node 7,
// whose links from nodes 5 and 6 are down, so it gets view change messages from nodes 2 to 4 only.
// Node 7 doesn't find out by itself that the leader is gone, and nothing is decided in the new view,
// so if it joined only after Q-1 view change messages, nothing would bring it to the new view.
// It ignores the heartbeats of the new leader, since they don't come from the leader of its view.
func TestNodeJoinsViewChangeOfFPlusOneNodes(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 7
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.LeaderHeartbeatTimeout = 5 * time.Second // the others find out soon that the leader is gone
		nodes = append(nodes, n)
	}
	left := nodes[6]
	left.Consensus.Config.LeaderHeartbeatTimeout = time.Hour   // it doesn't find out the leader is gone by itself
	left.Consensus.Config.ViewChangeTimeout = 10 * time.Second // once it joins, it syncs after this

	startNodes(nodes, network)
	nodes[0].Disconnect()
	nodes[4].DisconnectFrom(left.ID)
	nodes[5].DisconnectFrom(left.ID)

	// the others change the view, and the node joins the view change and syncs to the new view once it times out
	require.Eventually(t, func() bool {
		leader := nodes[1].Consensus.GetLeaderID()
		return leader != 1 && left.Consensus.GetLeaderID() == leader
	}, time.Minute, 100*time.Millisecond, "the node is not in the view of the others")
}

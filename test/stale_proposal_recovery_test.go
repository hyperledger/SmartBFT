// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestRestartDoesNotRestoreSyncedProposal covers a node whose WAL ends with a
// pre-prepare that it then received through a sync. A sync writes nothing to
// the WAL, so after a restart the record is older than the checkpoint. The node
// must not restore that proposal as in flight: it would reject every prepare of
// the next sequence as a wrong digest and miss it.
func TestRestartDoesNotRestoreSyncedProposal(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	const numberOfNodes = 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		nodes = append(nodes, newNode(uint64(i), network, t.Name(), testDir, false, 0))
	}

	restarted := nodes[numberOfNodes-1]

	// The node accepts pre-prepares but loses the votes, so it persists the
	// pre-prepare of sequence 1 and cannot decide it.
	var dropVotes atomic.Bool
	dropVotes.Store(true)
	restarted.LoseMessages(func(msg *smartbftprotos.Message) bool {
		return dropVotes.Load() && (msg.GetPrepare() != nil || msg.GetCommit() != nil)
	})

	processedProposal := make(chan struct{}, 1)
	synced := make(chan struct{}, 1)
	var wrongDigest atomic.Int64
	restarted.logger = restarted.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		switch {
		case strings.Contains(entry.Message, "Processed proposal with seq 1"):
			select {
			case processedProposal <- struct{}{}:
			default:
			}
		case strings.Contains(entry.Message, "Synchronizer returned with sequence 2"):
			select {
			case synced <- struct{}{}:
			default:
			}
		case strings.Contains(entry.Message, "Got wrong digest at processPrepares"):
			wrongDigest.Add(1)
		}
		return nil
	})).Sugar()
	restarted.Setup()

	startNodes(nodes, network)

	// The leader heartbeats more often, so the node notices it is behind
	// without waiting for the default interval.
	nodes[0].Consensus.Config.LeaderHeartbeatTimeout = 10 * time.Second
	nodes[0].Consensus.Config.LeaderHeartbeatCount = 10

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	waitForSignal(t, processedProposal, "the node to persist the pre-prepare of sequence 1")

	// The others decide sequence 2. The leader's heartbeat makes the node sync
	// to it, which delivers sequences 1 and 2 without writing to the WAL.
	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})
	waitForSignal(t, synced, "the node to sync to sequence 2")
	for _, n := range nodes {
		<-n.Delivered
		<-n.Delivered
	}

	dropVotes.Store(false)
	restarted.RestartSync(false)

	// The node takes part in sequence 3 by consensus, without rejecting the
	// prepares of the others.
	nodes[0].Submit(Request{ID: "3", ClientID: "alice"})
	for _, n := range nodes {
		select {
		case <-n.Delivered:
		case <-time.After(30 * time.Second):
			t.Fatalf("node %d did not deliver sequence 3", n.ID)
		}
	}
	require.Zero(t, wrongDigest.Load(), "the restarted node rejected prepares for the proposal it restored")
}

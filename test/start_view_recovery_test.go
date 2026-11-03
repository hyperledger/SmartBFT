// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRestartAfterNewViewAndProposalRestoresStartView(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	const numberOfNodes = 4
	nodes := make([]*App, 0, numberOfNodes)

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		if i == numberOfNodes {
			// Keep the node at PROPOSED: it accepts the pre-prepare and sends
			// its prepare, but cannot collect a quorum of prepares or commits.
			n.LoseMessages(func(msg *smartbftprotos.Message) bool {
				return msg.GetPrepare() != nil || msg.GetCommit() != nil
			})
		} else {
			// A non-nil filter suppresses the test app's check for prepares
			// when it signs an in-flight proposal.
			n.LoseMessages(func(*smartbftprotos.Message) bool { return false })
		}
		nodes = append(nodes, n)
	}

	restarted := nodes[numberOfNodes-1]
	proposalProcessed := make(chan struct{}, 1)

	baseLogger := restarted.Consensus.Logger.(*zap.SugaredLogger).Desugar()
	restarted.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Processed proposal with seq") {
			select {
			case proposalProcessed <- struct{}{}:
			default:
			}
		}
		return nil
	})).Sugar()

	startNodes(nodes, network)

	// Move every node to view 1. The restarted node writes a ViewChange and a
	// NewView record.
	for _, n := range nodes {
		n.Consensus.Complain(0, true)
	}

	require.Eventually(t, func() bool {
		return nodes[0].Consensus.GetLeaderID() == 2
	}, 10*time.Second, 10*time.Millisecond, "the cluster did not start view 1")

	// The leader of view 1 proposes. The restarted node processes the
	// pre-prepare. Saving the pre-prepare truncates the earlier ViewChange and
	// NewView records, so its WAL ends with ProposedRecord.
	nodes[1].Submit(Request{ID: "view 1 proposal", ClientID: "alice"})

	select {
	case <-proposalProcessed:
	case <-time.After(10 * time.Second):
		t.Fatal("the restarted node did not process the proposal in view 1")
	}

	restarted.RestartSync(false)

	require.Eventually(t, func() bool {
		return restarted.Consensus.GetLeaderID() != 0
	}, 10*time.Second, 10*time.Millisecond, "the restarted node did not start")

	require.Equal(t, uint64(2), restarted.Consensus.GetLeaderID(),
		"the restarted node started the controller in the view from the metadata, not the view from the WAL")
}

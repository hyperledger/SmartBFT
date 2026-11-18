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

// TestRestartInTheMiddleOfAViewChange makes sure that a node that restarts after it sent its view data for the next
// view doesn't run the view it left, and gets to the next view.
// The leader goes away, and the others change the view. Node 4 never gets the new view message, so its write ahead
// log still ends with its view change record when it restarts.
func TestRestartInTheMiddleOfAViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.LeaderHeartbeatTimeout = 5 * time.Second // the others find out soon that the leader is gone
		nodes = append(nodes, n)
	}
	restarted := nodes[3]
	restarted.LoseMessages(func(m *smartbftprotos.Message) bool {
		return newViewFor(m) == 1
	})

	var afterRestart atomic.Bool
	var sentViewData, startsOfTheLeftView, notStarted atomic.Int32
	restarted.logger = restarted.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if !afterRestart.Load() {
			if strings.Contains(entry.Message, "sent view data msg, with next view 1,") {
				sentViewData.Add(1)
			}
			return nil
		}
		if strings.Contains(entry.Message, "Starting view with number 0,") {
			startsOfTheLeftView.Add(1)
		}
		if strings.Contains(entry.Message, "Not starting view 0, which the node left") {
			notStarted.Add(1)
		}
		return nil
	})).Sugar()
	// The node's view changer gets no ticks before the restart, so that its view change doesn't time out and sync,
	// which would leave a later record in its write ahead log.
	restarted.viewChangeTime = make(chan time.Time)
	restarted.Setup() // so that the consensus uses the logger and the clock

	startNodes(nodes, network)
	nodes[0].Disconnect()

	require.Eventually(t, func() bool {
		return sentViewData.Load() > 0 && nodes[1].Consensus.GetLeaderID() == 2 && nodes[2].Consensus.GetLeaderID() == 2
	}, time.Minute, 100*time.Millisecond, "the view did not change")

	afterRestart.Store(true)
	restarted.RestartSync(false)
	require.Equal(t, int32(1), notStarted.Load(), "the node did not start in the view it left without running it")

	// a faster clock, so that the restored view change times out soon, and the node syncs to the new view
	done := make(chan struct{})
	defer close(done)
	var counter uint64
	accelerateTime([]*App{restarted}, done, false, true, &counter)

	require.Eventually(t, func() bool {
		return restarted.Consensus.GetLeaderID() == 2
	}, time.Minute, 100*time.Millisecond, "the node did not get to the new view")
	require.Zero(t, startsOfTheLeftView.Load(), "the node ran the view it left after the restart")
}

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

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestReconfigKeepsTheLeftView makes sure that a node keeps the views it left across a reconfiguration.
// Node 4 gets view change messages from nodes 2 and 3, so it leaves view 0 and sends its view data for view 1.
// The others stay in view 0 and decide a reconfiguration there, which node 4 gets by a sync once its view change
// times out. After the reconfiguration, node 4 starts in view 0, the view of its last decision, and must not run it.
func TestReconfigKeepsTheLeftView(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		nodes = append(nodes, newNode(uint64(i), network, t.Name(), testDir, false, 0))
	}
	left := nodes[3]

	var reconfigured atomic.Bool
	var sentViewData, startsOfTheLeftView, notStarted atomic.Int32
	left.logger = left.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "sent view data msg, with next view 1,") {
			sentViewData.Add(1)
		}
		if strings.Contains(entry.Message, "Starting reconfig") {
			reconfigured.Store(true)
		}
		if !reconfigured.Load() {
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
	// The node's view changer gets no ticks until the test gives them, so that its view change times out only then.
	left.viewChangeTime = make(chan time.Time)
	left.Setup() // so that the consensus uses the logger and the clock

	startNodes(nodes, network)

	// node 4 alone leaves view 0
	viewChange := &smartbftprotos.Message{
		Version: types.ProtocolVersion,
		Content: &smartbftprotos.Message_ViewChange{ViewChange: &smartbftprotos.ViewChange{NextView: 1}},
	}
	left.Consensus.HandleMessage(2, viewChange)
	left.Consensus.HandleMessage(3, viewChange)
	require.Eventually(t, func() bool {
		return sentViewData.Load() > 0
	}, time.Minute, 100*time.Millisecond, "the node did not leave view 0")

	// the others decide a reconfiguration in view 0
	newConfig := fastConfig
	newConfig.CollectTimeout = fastConfig.CollectTimeout * 2
	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})
	for _, n := range nodes[:3] {
		delivered(t, n)
	}

	// node 4's view change times out, and it syncs the reconfiguration
	done := make(chan struct{})
	defer close(done)
	var counter uint64
	accelerateTime([]*App{left}, done, false, true, &counter)

	require.Eventually(t, func() bool {
		return notStarted.Load() > 0 || startsOfTheLeftView.Load() > 0
	}, time.Minute, 100*time.Millisecond, "the node was not reconfigured")
	require.Zero(t, startsOfTheLeftView.Load(), "the node ran the view it left after the reconfiguration")
	require.NotZero(t, notStarted.Load())
}

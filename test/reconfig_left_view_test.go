// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
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

// TestReconfigKeepsTheLeftView makes sure that a node keeps the views it left across a reconfiguration, and across
// a restart after it.
// Node 4 gets view change messages from nodes 2 and 3, so it leaves view 0, or views 0 and 1. The others stay in
// view 0 and decide a reconfiguration there, which node 4 gets by a sync once its view change times out. After the
// reconfiguration node 4's last decision is in view 0, but it must not run a view it left. Then node 4 restarts, and
// the leader goes away, so that nodes 2 and 3 change to view 1:
//   - if node 4 left only view 0, all three change the view and decide a request;
//   - if node 4 left view 1 as well, it must not take part in the view change to view 1. Here nodes 2 and 3 can't
//     change the view without node 4, which a correct node never leaves alone, so no request is decided.
func TestReconfigKeepsTheLeftView(t *testing.T) {
	t.Parallel()

	for _, leftViews := range []uint64{1, 2} {
		t.Run(fmt.Sprintf("%d left views", leftViews), func(t *testing.T) {
			t.Parallel()
			testReconfigKeepsTheLeftViews(t, leftViews)
		})
	}
}

func testReconfigKeepsTheLeftViews(t *testing.T, leftViews uint64) {
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		// The view changers get no ticks until the test gives them, so that their view changes time out only then.
		n.viewChangeTime = make(chan time.Time)
		nodes = append(nodes, n)
	}
	leaver := nodes[3]

	var reconfigured atomic.Bool
	var sentViewData [3]atomic.Int32 // by the view it was sent for
	var startsOfALeftView, notStarted, viewChangesToView1 atomic.Int32
	leaver.logger = leaver.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		for view := range sentViewData {
			if strings.Contains(entry.Message, fmt.Sprintf("sent view data msg, with next view %d,", view)) {
				sentViewData[view].Add(1)
			}
		}
		if strings.Contains(entry.Message, "Starting reconfig") {
			reconfigured.Store(true)
		}
		if !reconfigured.Load() {
			return nil
		}
		for view := range leftViews {
			if strings.Contains(entry.Message, fmt.Sprintf("Starting view with number %d,", view)) {
				startsOfALeftView.Add(1)
			}
		}
		for view := range leftViews {
			if strings.Contains(entry.Message, fmt.Sprintf("Not starting view %d, which the node left", view)) {
				notStarted.Add(1)
			}
		}
		if strings.Contains(entry.Message, "is processing a view change message") &&
			strings.Contains(entry.Message, "with next view 1") {
			viewChangesToView1.Add(1)
		}
		return nil
	})).Sugar()
	for _, n := range nodes {
		n.Setup() // so that the consensus uses the clock, and the logger
	}

	startNodes(nodes, network)

	// node 4 alone leaves the views
	for view := uint64(1); view <= leftViews; view++ {
		viewChange := &smartbftprotos.Message{
			Version: types.ProtocolVersion,
			Content: &smartbftprotos.Message_ViewChange{ViewChange: &smartbftprotos.ViewChange{NextView: view}},
		}
		leaver.Consensus.HandleMessage(2, viewChange)
		leaver.Consensus.HandleMessage(3, viewChange)
		require.Eventually(t, func() bool {
			return sentViewData[view].Load() > 0
		}, time.Minute, 100*time.Millisecond, "the node did not send its view data for view %d", view)
	}
	for view := range leftViews {
		sentViewData[view].Store(0) // from here on, the node must not send view data for a view it left
	}

	// the others decide a reconfiguration in view 0
	newConfig := fastConfig
	newConfig.CollectTimeout = fastConfig.CollectTimeout * 2
	newConfig.LeaderHeartbeatTimeout = 5 * time.Second // so that the others find out soon when the leader goes away
	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})
	var reconfiguration *AppRecord
	for _, n := range nodes[:3] {
		reconfiguration = delivered(t, n)
	}

	keptTheLeftViews := func(when string) {
		t.Helper()
		require.Zero(t, startsOfALeftView.Load(), "the node ran a view it left %s", when)
		for view := range leftViews {
			require.Zero(t, sentViewData[view].Load(), "the node took part in a view change to view %d, which it left, %s", view, when)
		}
	}
	waitForTheLeftView := func(starts int32, when string) {
		t.Helper()
		require.Eventually(t, func() bool {
			return notStarted.Load() >= starts || startsOfALeftView.Load() > 0
		}, time.Minute, 100*time.Millisecond, "the node did not start %s", when)
		keptTheLeftViews(when)
	}

	// node 4's view change times out, and it syncs the reconfiguration
	leaverClock := make(chan struct{})
	var counter uint64
	accelerateTime([]*App{leaver}, leaverClock, false, true, &counter)
	waitForTheLeftView(1, "after the reconfiguration")
	require.Equal(t, reconfiguration, delivered(t, leaver), "the node did not sync the reconfiguration")
	close(leaverClock)

	// the leader goes away, and nodes 2 and 3 change to view 1, with one clock for the nodes that are left
	nodes[0].Disconnect()
	done := make(chan struct{})
	defer close(done)
	var othersCounter uint64
	accelerateTime(nodes[1:], done, false, true, &othersCounter)

	if leftViews == 1 {
		// all three change the view, and decide a request
		for _, n := range nodes[1:] {
			n.Submit(Request{ID: "11", ClientID: "alice"})
		}
		data := make([]*AppRecord, 0, numberOfNodes-1)
		for _, n := range nodes[1:] {
			data = append(data, delivered(t, n))
		}
		for i := range len(data) - 1 {
			require.Equal(t, data[i], data[i+1])
		}
		keptTheLeftViews("in the next view")

		// after a restart it is back in the view it entered
		leaver.RestartSync(false)
		require.Eventually(t, func() bool {
			return leaver.Consensus.GetLeaderID() == nodes[1].Consensus.GetLeaderID()
		}, time.Minute, 100*time.Millisecond, "the node is not in the view of the others after the restart")
		keptTheLeftViews("after the restart")
		return
	}

	// node 4 gets the view change messages for view 1, but doesn't take part in that view change
	require.Eventually(t, func() bool {
		return viewChangesToView1.Load() >= 2
	}, time.Minute, 100*time.Millisecond, "the node did not get view change messages for view 1")
	require.Never(t, func() bool {
		return sentViewData[1].Load() > 0
	}, 2*time.Second, 100*time.Millisecond, "the node took part in a view change to view 1, which it left")
	keptTheLeftViews("during the view change to view 1")

	// it keeps the views it left across a restart
	leaver.RestartSync(false)
	waitForTheLeftView(2, "after the restart")
}

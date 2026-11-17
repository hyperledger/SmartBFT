// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// TestNewViewIgnoredAfterRestart makes sure that a node that restarts in a view it entered by a new view message
// doesn't act on a new view message for that view again.
// The leader goes away, the others change the view, and then node 4 restarts and gets the new view message once more,
// either once it is back in the view, or as early as possible after it restarts.
func TestNewViewIgnoredAfterRestart(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		description    string
		waitForTheView bool
	}{
		{description: "once the node is back in the view", waitForTheView: true},
		{description: "right after the node restarts", waitForTheView: false},
	} {
		t.Run(test.description, func(t *testing.T) {
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

			var lock sync.Mutex
			var newView *smartbftprotos.Message
			restarted.LoseMessages(func(m *smartbftprotos.Message) bool {
				if newViewFor(m) == 1 {
					lock.Lock()
					defer lock.Unlock()
					newView = proto.Clone(m).(*smartbftprotos.Message)
				}
				return false // only observe the message
			})

			var afterRestart atomic.Bool
			var startsBefore, starts, viewChanges, ignored atomic.Int32
			restarted.logger = restarted.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if !afterRestart.Load() {
					if strings.Contains(entry.Message, "Starting view with number 1") {
						startsBefore.Add(1)
					}
					return nil
				}
				if strings.Contains(entry.Message, "Starting view with number 1") {
					starts.Add(1)
				}
				if strings.Contains(entry.Message, "ViewChanged, the new view is 1") {
					viewChanges.Add(1)
				}
				if strings.Contains(entry.Message, "already acted on a new view message for view 1") {
					ignored.Add(1)
				}
				return nil
			})).Sugar()
			restarted.Setup() // so that the consensus uses the logger

			startNodes(nodes, network)
			nodes[0].Disconnect()

			// wait until the node started the new view, so that this start is not counted as one after the restart
			require.Eventually(t, func() bool {
				lock.Lock()
				defer lock.Unlock()
				return newView != nil && startsBefore.Load() > 0
			}, time.Minute, 100*time.Millisecond, "the node did not change the view")

			lock.Lock()
			again := newView
			lock.Unlock()

			afterRestart.Store(true)
			restarted.RestartSync(false)
			if test.waitForTheView {
				require.Eventually(t, func() bool {
					return restarted.Consensus.GetLeaderID() == 2
				}, time.Minute, 100*time.Millisecond, "the node did not restart in the new view")
			}
			restarted.Consensus.HandleMessage(2, again)

			require.Eventually(t, func() bool {
				return ignored.Load() > 0
			}, 10*time.Second, 100*time.Millisecond, "the node did not ignore the new view message for the view it restarted in")
			require.Zero(t, viewChanges.Load(), "the node acted on the new view message after it restarted in the view")
			require.Equal(t, int32(1), starts.Load(), "the node started the view again after it restarted in it")
			require.Equal(t, uint64(2), restarted.Consensus.GetLeaderID(), "the node did not restart in the new view")
		})
	}
}

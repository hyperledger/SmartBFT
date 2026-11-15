// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// TestNodesDoNotReturnToALeftView makes sure that once a node sent its view data for the next view, it never starts
// the view it left again, and that a view change whose new leader never sends a new view moves on to a later view.
// With 7 nodes f is 2. The leader goes away, and the new view message of the leader of view 1 never arrives,
// so the view change times out, and the others sync and change the view again, to view 2.
func TestNodesDoNotReturnToALeftView(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	var lock sync.Mutex
	sentViewData := map[uint64]bool{} // the nodes that sent their view data for view 1
	var restarted []string

	numberOfNodes := 7
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.ViewChangeTimeout = 10 * time.Second // the view change to view 1 times out sooner

		id := n.ID
		baseLogger := n.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		n.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			lock.Lock()
			defer lock.Unlock()
			if strings.Contains(entry.Message, "sent view data msg, with next view 1") {
				sentViewData[id] = true
			}
			if sentViewData[id] && strings.Contains(entry.Message, "Starting view with number 0") {
				restarted = append(restarted, fmt.Sprintf("node %d", id))
			}
			return nil
		})).Sugar()

		n.LoseMessages(func(m *smartbftprotos.Message) bool {
			return newViewFor(m) == 1 // the leader of view 1 doesn't send its new view to anyone
		})
		nodes = append(nodes, n)
	}

	startNodes(nodes, network)
	nodes[0].Disconnect()

	// the others forward the request to the leader, complain, and change the view twice
	for _, n := range nodes[1:] {
		n.Submit(Request{ID: "1", ClientID: "alice"})
	}
	data := make([]*AppRecord, 0, numberOfNodes-2)
	for _, n := range nodes[2:] {
		data = append(data, delivered(t, n))
	}
	for i := range len(data) - 1 {
		require.Equal(t, data[i], data[i+1])
	}
	md := &smartbftprotos.ViewMetadata{}
	require.NoError(t, proto.Unmarshal(data[0].Metadata, md))
	require.GreaterOrEqual(t, md.GetViewId(), uint64(2), "the request was decided in a view whose leader didn't send a new view")

	lock.Lock()
	defer lock.Unlock()
	require.NotEmpty(t, sentViewData, "no node sent its view data for view 1")
	require.Empty(t, restarted, "nodes started view 0 again after they sent their view data for view 1")
}

// newViewFor returns the view that the given new view message is for, or 0 if it is not a new view message.
func newViewFor(m *smartbftprotos.Message) uint64 {
	signed := m.GetNewView().GetSignedViewData()
	if len(signed) == 0 {
		return 0
	}
	vd := &smartbftprotos.ViewData{}
	if err := proto.Unmarshal(signed[0].GetRawViewData(), vd); err != nil {
		return 0
	}
	return vd.GetNextView()
}

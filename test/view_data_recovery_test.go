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
	"google.golang.org/protobuf/proto"
)

// TestRestoredInFlightProposalReachesViewData covers the recovery path from the
// WAL to the view data message. A node prepares a proposal, joins a view
// change, and restarts while its WAL ends with the ViewChange record. After the
// restart the node must send ViewData with the prepared proposal.
func TestRestoredInFlightProposalReachesViewData(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	const numberOfNodes = 4
	nodes := make([]*App, 0, numberOfNodes)

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		if i == numberOfNodes {
			// The restarted node prepares the proposal but does not receive
			// commits, so it cannot decide it. It also does not receive the
			// NewView, so it stays in the view change until it restarts.
			n.LoseMessages(func(msg *smartbftprotos.Message) bool {
				return msg.GetCommit() != nil || msg.GetNewView() != nil
			})
		} else {
			// The test app panics when it signs an in-flight proposal without
			// having seen prepares. A non-nil filter suppresses that check.
			n.LoseMessages(func(*smartbftprotos.Message) bool { return false })
		}
		nodes = append(nodes, n)
	}

	restarted := nodes[numberOfNodes-1]
	prepared := make(chan struct{}, 1)
	viewDataSent := make(chan struct{}, 1)

	baseLogger := restarted.Consensus.Logger.(*zap.SugaredLogger).Desugar()
	restarted.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		switch {
		case strings.Contains(entry.Message, "Processed prepares for proposal with seq 1"):
			select {
			case prepared <- struct{}{}:
			default:
			}
		case strings.Contains(entry.Message, "sent view data msg"):
			select {
			case viewDataSent <- struct{}{}:
			default:
			}
		}
		return nil
	})).Sugar()

	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "prepared", ClientID: "alice"})

	select {
	case <-prepared:
	case <-time.After(10 * time.Second):
		t.Fatal("the node did not prepare the proposal")
	}

	// Make the other nodes start a view change. The restarted node receives
	// their view change messages, writes its own ViewChange record, and sends
	// the first ViewData.
	for i := range numberOfNodes - 1 {
		nodes[i].Consensus.Complain(0, true)
	}

	select {
	case <-viewDataSent:
	case <-time.After(10 * time.Second):
		t.Fatal("the node did not send view data before the restart")
	}

	// Observe outgoing ViewData only after the restart.
	var capture atomic.Bool
	viewData := make(chan *smartbftprotos.ViewData, 1)
	for target := uint64(1); target <= numberOfNodes; target++ {
		if target == restarted.ID {
			continue
		}
		restarted.MutateSend(target, func(_ uint64, msg *smartbftprotos.Message) {
			if !capture.Load() {
				return
			}
			signed := msg.GetViewData()
			if signed == nil {
				return
			}
			decoded := &smartbftprotos.ViewData{}
			if err := proto.Unmarshal(signed.GetRawViewData(), decoded); err != nil {
				return
			}
			select {
			case viewData <- decoded:
			default:
			}
		})
	}

	// Drop a ViewData that may still be in flight from the old instance.
	select {
	case <-viewData:
	default:
	}

	capture.Store(true)
	restarted.RestartSync(false)

	var vd *smartbftprotos.ViewData
	select {
	case vd = <-viewData:
	case <-time.After(10 * time.Second):
		t.Fatal("the restarted node did not send view data")
	}

	require.Equal(t, uint64(1), vd.GetNextView())
	require.NotNil(t, vd.GetInFlightProposal(), "the restarted node lost the in-flight proposal")
	require.True(t, vd.GetInFlightPrepared(), "the restarted node lost the prepared flag")
}

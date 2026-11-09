// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestControllerDecideViewBookkeeping(t *testing.T) {
	t.Run("view running", func(t *testing.T) {
		view := &View{abortChan: make(chan struct{})}
		controller := newDecidingController(t, view)

		controller.decide(newDecision(t, view))
		require.Equal(t, uint64(1), controller.getCurrentDecisionsInView())
		require.Len(t, controller.leaderToken, 1)
	})

	t.Run("view aborted", func(t *testing.T) {
		// The decision was made in the current view, so it counts in that view,
		// but the aborted view must not lead because of it.
		view := &View{abortChan: make(chan struct{})}
		controller := newDecidingController(t, view)
		view.stop()

		controller.decide(newDecision(t, view))
		require.Equal(t, uint64(1), controller.getCurrentDecisionsInView())
		require.Empty(t, controller.leaderToken)
	})

	t.Run("view replaced", func(t *testing.T) {
		view := &View{abortChan: make(chan struct{})}
		view.stop()
		controller := newDecidingController(t, &View{abortChan: make(chan struct{})})

		controller.decide(newDecision(t, view))
		require.Equal(t, uint64(0), controller.getCurrentDecisionsInView())
		require.Empty(t, controller.leaderToken)
	})
}

// TestDecideDoesNotDropDecisionOnAbortedView covers the hand-off between the view
// and the controller: a view that is being aborted must not stay blocked on a full
// decision channel, but the committed decision must still reach the controller.
func TestDecideDoesNotDropDecisionOnAbortedView(t *testing.T) {
	view := &View{abortChan: make(chan struct{})}
	controller := &Controller{
		currView:     view,
		decisionChan: make(chan decision, 1),
		stopChan:     make(chan struct{}),
	}
	controller.decisionChan <- decision{proposal: types.Proposal{Payload: []byte("queued")}}

	decided := make(chan struct{})
	go func() {
		controller.Decide(types.Proposal{Payload: []byte("committed")}, nil, nil)
		close(decided)
	}()

	select {
	case <-decided:
		t.Fatal("Decide returned before the view was aborted")
	case <-time.After(50 * time.Millisecond):
	}

	view.stop()

	select {
	case <-decided:
	case <-time.After(time.Second):
		t.Fatal("Decide did not return after the view was aborted")
	}

	queued := <-controller.decisionChan
	require.Equal(t, []byte("queued"), queued.proposal.Payload)

	select {
	case committed := <-controller.decisionChan:
		require.Equal(t, []byte("committed"), committed.proposal.Payload)
	case <-time.After(time.Second):
		t.Fatal("the committed decision was dropped")
	}
}

// newDecidingController returns a controller whose current view is the given view,
// and whose own ID is the leader of the current view, so that a delivered decision
// would normally acquire the leader token.
func newDecidingController(t *testing.T, currView Proposer) *Controller {
	metadata, err := proto.Marshal(&protos.ViewMetadata{})
	require.NoError(t, err)

	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{Metadata: metadata}, nil)

	return &Controller{
		ID:          1,
		N:           4,
		NodesList:   []uint64{1, 2, 3, 4},
		Logger:      zap.NewNop().Sugar(),
		Deliver:     applicationFunc(func(types.Proposal, []types.Signature) types.Reconfig { return types.Reconfig{} }),
		Verifier:    noopVerifier{},
		Checkpoint:  checkpoint,
		currView:    currView,
		stopChan:    make(chan struct{}),
		leaderToken: make(chan struct{}, 1),
	}
}

func newDecision(t *testing.T, view Proposer) decision {
	metadata, err := proto.Marshal(&protos.ViewMetadata{})
	require.NoError(t, err)

	return decision{
		proposal:  types.Proposal{Metadata: metadata},
		view:      view,
		delivered: make(chan struct{}),
	}
}

// These tests live in package bft to reach the unexported decide, so they
// cannot use internal/bft/mocks without creating an import cycle.

type applicationFunc func(types.Proposal, []types.Signature) types.Reconfig

func (f applicationFunc) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	return f(proposal, signatures)
}

type noopVerifier struct{}

func (noopVerifier) VerificationSequence() uint64 {
	return 0
}

func (noopVerifier) VerifyProposal(types.Proposal) ([]types.RequestInfo, error) {
	panic("unexpected VerifyProposal call")
}

func (noopVerifier) VerifyRequest([]byte) (types.RequestInfo, error) {
	panic("unexpected VerifyRequest call")
}

func (noopVerifier) VerifyConsenterSig(types.Signature, types.Proposal) ([]byte, error) {
	panic("unexpected VerifyConsenterSig call")
}

func (noopVerifier) VerifySignature(types.Signature) error {
	panic("unexpected VerifySignature call")
}

func (noopVerifier) RequestsFromProposal(types.Proposal) []types.RequestInfo {
	panic("unexpected RequestsFromProposal call")
}

func (noopVerifier) AuxiliaryData([]byte) []byte {
	panic("unexpected AuxiliaryData call")
}

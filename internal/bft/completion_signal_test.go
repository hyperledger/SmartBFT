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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestControllerDecideDoesNotBlockIfDeliveryWaiterLeft(t *testing.T) {
	view := &View{abortChan: make(chan struct{})}
	controller := newDecidingController(t, view)

	requireReturns(t, func() {
		controller.decide(newDecision(t, view))
	})
	assert.Equal(t, uint64(1), controller.getCurrentDecisionsInView())
}

func TestControllerDecideSkipsViewBookkeepingIfDecidingViewIsGone(t *testing.T) {
	t.Run("view aborted", func(t *testing.T) {
		view := &View{abortChan: make(chan struct{})}
		controller := newDecidingController(t, view)
		view.stop()

		requireReturns(t, func() {
			controller.decide(newDecision(t, view))
		})
		assert.Equal(t, uint64(0), controller.getCurrentDecisionsInView())
		assert.Empty(t, controller.leaderToken)
	})

	t.Run("view replaced", func(t *testing.T) {
		view := &View{abortChan: make(chan struct{})}
		view.stop()
		controller := newDecidingController(t, &View{abortChan: make(chan struct{})})

		requireReturns(t, func() {
			controller.decide(newDecision(t, view))
		})
		assert.Equal(t, uint64(0), controller.getCurrentDecisionsInView())
		assert.Empty(t, controller.leaderToken)
	})
}

func TestViewChangerDecideDoesNotBlockIfInFlightWaiterLeft(t *testing.T) {
	inFlightView := &View{abortChan: make(chan struct{})}
	viewChanger := &ViewChanger{
		Logger:        zap.NewNop().Sugar(),
		Application:   applicationFunc(func(types.Proposal, []types.Signature) types.Reconfig { return types.Reconfig{} }),
		RequestsTimer: noopRequestsTimer{},
		Pruner:        noopPruner{},
	}
	// Unbuffered and unread: the attempt's waiter has already left.
	attempt := &inFlightAttempt{
		decideCh: make(chan struct{}),
		syncCh:   make(chan struct{}),
		viewRef:  inFlightView,
	}

	requireReturns(t, func() {
		viewChanger.decideInFlight(attempt, types.Proposal{}, nil, nil)
	})
	assert.True(t, inFlightView.Stopped())
}

func TestViewChangerSyncDoesNotBlockIfInFlightWaiterLeft(t *testing.T) {
	viewChanger := &ViewChanger{
		Logger:       zap.NewNop().Sugar(),
		Synchronizer: synchronizerFunc(func() {}),
	}
	// Unbuffered and unread: the attempt's waiter has already left.
	attempt := &inFlightAttempt{
		decideCh: make(chan struct{}),
		syncCh:   make(chan struct{}),
	}

	requireReturns(t, func() {
		viewChanger.syncInFlight(attempt)
	})
}

// newDecidingController returns a controller whose current view is the given view,
// and whose own ID is the leader of the current view, so that a delivered decision
// would normally acquire the leader token.
func newDecidingController(t *testing.T, currView Proposer) *Controller {
	metadata, err := proto.Marshal(&protos.ViewMetadata{})
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	return decision{
		proposal:  types.Proposal{Metadata: metadata},
		view:      view,
		delivered: make(chan struct{}),
	}
}

func requireReturns(t *testing.T, f func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("function blocked")
	}
}

// These tests live in package bft to reach unexported completion paths, so they
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

type noopRequestsTimer struct{}

func (noopRequestsTimer) StopTimers() {}

func (noopRequestsTimer) RestartTimers() {}

func (noopRequestsTimer) RemoveRequest(types.RequestInfo) error {
	return nil
}

type noopPruner struct{}

func (noopPruner) MaybePruneRevokedRequests() {}

type synchronizerFunc func()

func (f synchronizerFunc) Sync() {
	f()
}

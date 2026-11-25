// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestStartViewWithACarriedProposal makes sure that a view that starts with a carried proposal is built with it, and
// that its leader doesn't take the leader token, since the view starts with that proposal instead of one from the
// leader.
func TestStartViewWithACarriedProposal(t *testing.T) {
	carried := &types.Proposal{Payload: []byte{1}}

	for _, test := range []struct {
		description string
		carried     *types.Proposal
		token       bool
	}{
		{description: "with a carried proposal", carried: carried},
		{description: "without one", token: true},
	} {
		t.Run(test.description, func(t *testing.T) {
			controller, builder := newCarryingController(t)
			controller.ID = 1 // the leader of view 0

			controller.startView(5, test.carried)
			require.Equal(t, []*types.Proposal{test.carried}, builder.carriedProposals())
			require.Equal(t, []uint64{0}, builder.started())
			require.Equal(t, test.token, len(controller.leaderToken) == 1)
		})
	}
}

// TestViewChangedPassesTheCarriedProposal makes sure that the carried proposal of a view change reaches the view that
// the controller starts for it.
func TestViewChangedPassesTheCarriedProposal(t *testing.T) {
	controller, builder := newCarryingController(t)
	controller.ID = 3
	controller.viewChange = make(chan viewInfo, 1)
	done := make(chan struct{})
	go func() {
		controller.run()
		close(done)
	}()
	stop := sync.OnceFunc(func() {
		close(controller.stopChan)
		<-done
	})
	t.Cleanup(stop)

	carried := &types.Proposal{Payload: []byte{1}}
	controller.ViewChanged(1, 5, carried)
	require.Eventually(t, func() bool { return len(builder.carriedProposals()) == 1 }, time.Minute, 10*time.Millisecond)
	stop()
	require.Equal(t, []uint64{1}, builder.started())
	require.Equal(t, []*types.Proposal{carried}, builder.carriedProposals())
}

// newCarryingController returns a controller in view 0 whose own ID leads view 1, with a view builder that records
// what it built.
func newCarryingController(t *testing.T) (*Controller, *recordingProposerBuilder) {
	builder := &recordingProposerBuilder{}
	controller := newDecidingController(t, &View{abortChan: make(chan struct{})})
	controller.ID = 2
	controller.Batcher = abortTestBatcher{}
	controller.LeaderMonitor = abortTestLeaderMonitor{}
	controller.ProposerBuilder = builder
	controller.ViewSequences = &atomic.Value{}
	controller.ViewSequences.Store(ViewSequence{ViewActive: true, ProposalSeq: 5})
	t.Cleanup(builder.abortAll)

	return controller, builder
}

// TestRestartKeepsTheCarriedProposal makes sure that a view that started with a carried proposal starts with it again
// when it is restarted at its sequence, so that its leader doesn't propose another proposal there, and that a restart
// at a later sequence doesn't.
func TestRestartKeepsTheCarriedProposal(t *testing.T) {
	controller, builder := newCarryingController(t)
	carried := &types.Proposal{Payload: []byte{1}}

	controller.changeView(1, 5, 0, carried)
	builder.views[0].Abort() // the view stops, and a sync that found nothing newer restarts it
	controller.changeView(1, 5, 0, nil)
	require.Equal(t, []*types.Proposal{carried, carried}, builder.carriedProposals())
	require.Empty(t, controller.leaderToken)

	// the carried proposal was decided
	builder.views[1].Abort()
	controller.changeView(1, 6, 1, nil)
	require.Equal(t, []*types.Proposal{carried, carried, nil}, builder.carriedProposals())
	require.Len(t, controller.leaderToken, 1)
}

// TestCarriedProposalRestartsAViewThatStartedWithout makes sure that a carried proposal for a view that a sync already
// started without it restarts the view with it, and that the same carried proposal again doesn't.
func TestCarriedProposalRestartsAViewThatStartedWithout(t *testing.T) {
	controller, builder := newCarryingController(t)
	carried := &types.Proposal{Payload: []byte{1}}

	controller.changeView(1, 5, 0, nil)
	<-controller.leaderToken // the leader got the token, as the view started without a carried proposal
	controller.changeView(1, 5, 0, carried)
	require.Equal(t, []*types.Proposal{nil, carried}, builder.carriedProposals())
	require.Empty(t, controller.leaderToken)

	controller.changeView(1, 5, 0, carried)
	require.Equal(t, []*types.Proposal{nil, carried}, builder.carriedProposals(), "the view is already running")
}

// TestViewWithACarriedProposalIsNotRestartedForNothing makes sure that a view that started with a carried proposal is
// not restarted for a copy of that proposal, nor at a later sequence, where no proposal is carried.
func TestViewWithACarriedProposalIsNotRestartedForNothing(t *testing.T) {
	controller, builder := newCarryingController(t)
	carried := &types.Proposal{Payload: []byte{1}}
	controller.changeView(1, 5, 0, carried)

	copied := *carried
	controller.changeView(1, 5, 0, &copied)
	require.Len(t, builder.views, 1, "the view already runs with that carried proposal")

	// the view decided the carried proposal, which doesn't count in it, and is at sequence 6
	controller.ViewSequences.Store(ViewSequence{ViewActive: true, ProposalSeq: 6})
	controller.changeView(1, 6, 0, nil)
	require.Len(t, builder.views, 1, "the view at sequence 6 already runs")
}

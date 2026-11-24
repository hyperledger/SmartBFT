// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// stubSynchronizer returns a fixed SyncResponse.
type stubSynchronizer struct {
	response types.SyncResponse
}

func (s *stubSynchronizer) Sync() types.SyncResponse { return s.response }

// stubComm is a no-op implementation of api.Comm.
type stubComm struct{}

func (s *stubComm) SendConsensus(targetID uint64, m *protos.Message) {}
func (s *stubComm) SendTransaction(targetID uint64, request []byte)  {}
func (s *stubComm) Nodes() []uint64                                  { return []uint64{1, 2, 3, 4} }
func (s *stubComm) BroadcastConsensus(m *protos.Message)             {}

// TestSyncDecisionsInView tests that the sync() method returns the correct
// DecisionsInView value in two scenarios:
//   - same height: sync returns the same sequence as the controller, so
//     newDecisionsInView must come from the controller's in-memory state
//     (getCurrentDecisionsInView), not remain zero-initialized.
//   - higher height: sync returns a higher sequence, so newDecisionsInView
//     is overridden from the sync response metadata (+1).
func TestSyncDecisionsInView(t *testing.T) {
	const (
		controllerSeq       = uint64(2365908)
		controllerView      = uint64(782)
		controllerDecisions = uint64(9578)
	)

	// newController creates a minimal Controller with only the fields
	// needed by sync(). The checkpoint metadata sets the controller's
	// current sequence, and the synchronizer controls what sync returns.
	newController := func(t *testing.T, syncResponse types.SyncResponse) *Controller {
		t.Helper()

		basicLog, err := zap.NewDevelopment()
		require.NoError(t, err)
		log := basicLog.Sugar()

		// Checkpoint metadata determines latestSeq() return value.
		checkpoint := &types.Checkpoint{}
		checkpoint.Set(types.Proposal{
			Metadata: MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence:  controllerSeq,
				ViewId:          controllerView,
				DecisionsInView: controllerDecisions,
			}),
		}, nil)

		// StateCollector with very short timeout so fetchState() returns nil
		// without needing real consensus broadcast responses.
		collector := &StateCollector{
			SelfID:         1,
			N:              4,
			Logger:         log,
			CollectTimeout: time.Millisecond,
		}
		collector.Start()
		t.Cleanup(collector.Stop)

		c := &Controller{
			ID:                  1,
			N:                   4,
			Logger:              log,
			Comm:                &stubComm{},
			Synchronizer:        &stubSynchronizer{response: syncResponse},
			Checkpoint:          checkpoint,
			Collector:           collector,
			InFlight:            &InFlightData{},
			ViewChanger:         &ViewChanger{},
			currViewNumber:      controllerView,
			currDecisionsInView: controllerDecisions + 1, // checkpoint stores N, decide() increments to N+1
		}
		// sync() uses grabSyncToken/relinquishSyncToken which need syncChan.
		c.syncChan = make(chan struct{}, 1)

		return c
	}

	t.Run("same_height_must_preserve_decisions", func(t *testing.T) {
		// Synchronizer returns the same sequence as the controller.
		// This simulates the "already at target height" scenario where
		// the orderer is not behind but sync was triggered anyway.
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq,
						ViewId:          controllerView,
						DecisionsInView: controllerDecisions,
					}),
					VerificationSequence: 0,
				},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		})

		viewNum, seq, decisions := c.sync()

		require.Equal(t, controllerView, viewNum, "view number should be preserved")
		require.Equal(t, controllerSeq+1, seq, "proposal sequence should be controllerSeq+1")
		// newDecisionsInView should be initialized from the controller's in-memory
		// state (getCurrentDecisionsInView), which is controllerDecisions+1.
		require.Equal(t, controllerDecisions+1, decisions,
			"DecisionsInView must be preserved when sync returns same height")
	})

	t.Run("higher_height_overrides_decisions", func(t *testing.T) {
		// Synchronizer returns a higher sequence than the controller.
		// In this case newDecisionsInView is overridden from the sync response (+1).
		syncDecisions := controllerDecisions + 1
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq + 1,
						ViewId:          controllerView,
						DecisionsInView: syncDecisions,
					}),
					VerificationSequence: 0,
				},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		})

		viewNum, seq, decisions := c.sync()

		require.Equal(t, controllerView, viewNum, "view number should be preserved")
		require.Equal(t, controllerSeq+2, seq, "proposal sequence should be syncSeq+1")
		require.Equal(t, syncDecisions+1, decisions,
			"DecisionsInView should be latestDecisionDecisions+1 when sync returns higher height")
	})

	t.Run("higher_height_of_an_earlier_view", func(t *testing.T) {
		// The decision was made before the view changed, or was carried into this view, so it doesn't count in the
		// view, which keeps its decisions, not the decision's.
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq + 1,
						ViewId:          controllerView - 1,
						DecisionsInView: 1,
					}),
				},
			},
		})

		viewNum, seq, decisions := c.sync()

		require.Equal(t, controllerView, viewNum)
		require.Equal(t, controllerSeq+2, seq)
		require.Equal(t, controllerDecisions+1, decisions)
	})

	t.Run("same_height_of_an_earlier_view", func(t *testing.T) {
		// Nothing new to report.
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq,
						ViewId:          controllerView - 1,
						DecisionsInView: controllerDecisions,
					}),
				},
			},
		})

		viewNum, seq, decisions := c.sync()

		require.Zero(t, viewNum)
		require.Zero(t, seq)
		require.Zero(t, decisions)
	})
}

// TestSyncOnStartPastADecisionOfAnEarlierView makes sure that the sync when the controller starts compares the decision
// it finds with the view the controller starts in, so a decision of an earlier view doesn't count in it.
func TestSyncOnStartPastADecisionOfAnEarlierView(t *testing.T) {
	// The node starts in view 6 at sequence 21. The others decided sequence 21, proposed in view 5, and are already
	// at sequence 23. Without the start view, the sync would take the view and decisions from the decision; if the
	// others were at 22, it would save a new view record for view 6 with no decisions, which hides that.
	fixture := newSyncFixture(t, 20, 6, 21, 5)
	controller := fixture.controller
	controller.currViewNumber = 0 // not started yet
	controller.currDecisionsInView = 0
	controller.Verifier = noopVerifier{}
	controller.Comm.(*stateRespondingComm).seq = 23

	view, seq, decisions := controller.syncOnStart(6, 21, 0)
	require.Equal(t, uint64(6), view)
	require.Equal(t, uint64(22), seq)
	require.Zero(t, decisions)
}

// TestSyncPastADecisionOfAnEarlierView makes sure that a node that syncs a decision it didn't have, made before its
// view or carried into it, restarts its view after that decision with the decisions the view already had, which is
// what the leader's next proposal expects.
func TestSyncPastADecisionOfAnEarlierView(t *testing.T) {
	for _, decisions := range []uint64{0, 2} {
		t.Run(fmt.Sprintf("%d decisions in the view", decisions), func(t *testing.T) {
			// The node is in view 6 at sequence 11. The others decided sequence 11, proposed in view 5, and are in
			// view 6 at sequence 12. The decision doesn't count in view 6, which keeps its decisions.
			controller, running, builder := newRunningSyncController(t, 11, decisions)

			controller.changeView(controller.sync())

			require.True(t, running.Stopped(), "the view at sequence 11 must not keep running")
			require.Len(t, builder.views, 1)
			restarted := builder.views[0]
			require.Equal(t, uint64(6), restarted.Number)
			require.Equal(t, uint64(3), restarted.LeaderID)
			require.Equal(t, uint64(12), restarted.ProposalSequence)
			require.Equal(t, decisions, restarted.DecisionsInView)
		})
	}
}

// TestChangeViewDoesNotRestartAViewThatIsAhead makes sure that a sync doesn't move a running view back. The view moves
// to its next sequence before the controller delivers its decision and counts it, so it can be ahead of the checkpoint,
// with decisions the sync doesn't count. A view at the sequence the sync returns restarts only if its count differs.
func TestChangeViewDoesNotRestartAViewThatIsAhead(t *testing.T) {
	// The checkpoint is at sequence 10, and the sync returns decision 11, so the view is to run at 12. The view
	// already decided 11, or 11 and 12, but the controller didn't deliver them yet.
	for _, test := range []struct {
		viewSeq, syncDecisions uint64
		restarted              bool
	}{
		{viewSeq: 12, syncDecisions: 0},
		{viewSeq: 12, syncDecisions: 1, restarted: true},
		{viewSeq: 13, syncDecisions: 0},
		{viewSeq: 13, syncDecisions: 1},
	} {
		t.Run(fmt.Sprintf("view at %d, %d decisions", test.viewSeq, test.syncDecisions), func(t *testing.T) {
			controller, running, builder := newRunningSyncController(t, test.viewSeq, 0)

			controller.changeView(6, 12, test.syncDecisions)

			require.Equal(t, test.restarted, running.Stopped())
			require.Equal(t, test.restarted, len(builder.views) == 1)
		})
	}
}

// TestSyncOnStartPrunesAStaleInFlightProposal makes sure that the sync when the controller starts removes an in-flight
// proposal that is already decided, also when the decision is of an earlier view and the sync has nothing else to
// report.
func TestSyncOnStartPrunesAStaleInFlightProposal(t *testing.T) {
	// The node restarts in view 6 at sequence 11, and its write ahead log still has the proposal of sequence 10, from
	// view 5, in flight. The synchronizer returns that decision, and the others are in view 6.
	fixture := newSyncFixture(t, 10, 5, 10, 5)
	controller := fixture.controller
	controller.currViewNumber = 0 // not started yet
	controller.Verifier = noopVerifier{}
	controller.InFlight.StoreProposal(types.Proposal{
		Metadata: MarshalOrPanic(&protos.ViewMetadata{ViewId: 5, LatestSequence: 10}),
	})

	view, seq, decisions := controller.syncOnStart(6, 11, 0)
	require.Equal(t, uint64(6), view)
	require.Equal(t, uint64(11), seq)
	require.Zero(t, decisions)
	require.Nil(t, controller.InFlight.InFlightProposal())
}

// newRunningSyncController returns the controller of newSyncFixture, in view 6 with the given decisions in it, at
// sequence 10, and the syncs it runs return sequence 11 from view 5. Its running view, led by node 3, is at the given
// sequence, and the views it starts are built by the returned builder.
func newRunningSyncController(t *testing.T, viewSeq, decisions uint64) (*Controller, *View, *recordingProposerBuilder) {
	controller := newSyncFixture(t, 10, 6, 11, 5).controller
	controller.currDecisionsInView = decisions
	running := &View{Number: 6, LeaderID: 3, abortChan: make(chan struct{})}
	controller.currView = running
	controller.ViewSequences = &atomic.Value{}
	controller.ViewSequences.Store(ViewSequence{ViewActive: true, ProposalSeq: viewSeq})
	builder := &recordingProposerBuilder{}
	controller.ProposerBuilder = builder
	controller.LeaderMonitor = abortTestLeaderMonitor{}
	t.Cleanup(func() {
		for _, view := range builder.views {
			view.Abort()
		}
	})

	return controller, running, builder
}

// stateRespondingComm answers the state transfer request of sync() on behalf of
// every other node with the view and sequence the cluster claims to be at.
type stateRespondingComm struct {
	stubComm
	collector *StateCollector
	view      uint64
	seq       uint64
}

func (c *stateRespondingComm) SendConsensus(targetID uint64, m *protos.Message) {
	if m.GetStateTransferRequest() == nil {
		return
	}

	c.collector.HandleMessage(targetID, &protos.Message{
		Content: &protos.Message_StateTransferResponse{
			StateTransferResponse: &protos.StateTransferResponse{
				ViewNum:  c.view,
				Sequence: c.seq,
			},
		},
	})
}

// syncState records what sync() saves and when.
type syncState struct {
	*StateRecorder
	proposer        *View
	savedAfterAbort bool
	beforeSave      func()
}

func (s *syncState) Save(msg *protos.SavedMessage, truncateTo bool) error {
	if s.proposer != nil && !s.proposer.Stopped() {
		s.savedAfterAbort = false
	}

	if s.beforeSave != nil {
		s.beforeSave()
	}

	return s.StateRecorder.Save(msg, truncateTo)
}

// syncFixture is a Controller whose sync() runs all the way to saving a record
// of the new view, together with the state the test can inspect.
type syncFixture struct {
	controller *Controller
	state      *syncState
}

// newSyncFixture builds a controller that sits at controllerView and
// controllerSeq, and whose synchronizer returns a confirmed decision at syncSeq
// and syncView. The state transfer request is answered with a view one higher
// than syncView and syncSeq+1 as its sequence, which is what makes sync() save a
// record of the new view.
func newSyncFixture(t *testing.T, controllerSeq, controllerView, syncSeq, syncView uint64) *syncFixture {
	t.Helper()

	log := newQuietLogger()

	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{
		Metadata: MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence:  controllerSeq,
			ViewId:          controllerView,
			DecisionsInView: 1,
		}),
	}, []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}})

	collector := &StateCollector{
		SelfID:         1,
		N:              4,
		Logger:         log,
		CollectTimeout: 5 * time.Second,
	}
	collector.Start()
	t.Cleanup(collector.Stop)

	state := &syncState{
		StateRecorder:   &StateRecorder{},
		savedAfterAbort: true,
	}

	controller := &Controller{
		ID:        1,
		N:         4,
		NodesList: []uint64{1, 2, 3, 4},
		Logger:    log,
		Comm: &stateRespondingComm{
			collector: collector,
			view:      syncView + 1,
			seq:       syncSeq + 1,
		},
		Synchronizer: &stubSynchronizer{response: types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence: syncSeq,
						ViewId:         syncView,
					}),
				},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		}},
		Checkpoint:          checkpoint,
		Collector:           collector,
		InFlight:            &InFlightData{},
		ViewChanger:         &ViewChanger{informChan: make(chan uint64, 1)},
		State:               state,
		currViewNumber:      controllerView,
		currDecisionsInView: 1,
	}
	controller.syncChan = make(chan struct{}, 1)

	return &syncFixture{controller: controller, state: state}
}

// TestSyncAbortsViewBeforeSavingNewView covers the write side of the record
// ordering: once the record of the new view is written, the view that was
// running when the sync started must not append anything after it.
func TestSyncAbortsViewBeforeSavingNewView(t *testing.T) {
	const (
		controllerSeq  = uint64(10)
		controllerView = uint64(5)
		syncSeq        = uint64(12)
		syncView       = uint64(6)
	)

	fixture := newSyncFixture(t, controllerSeq, controllerView, syncSeq, syncView)
	proposer := &View{Number: controllerView, abortChan: make(chan struct{})}
	fixture.controller.currView = proposer
	fixture.state.proposer = proposer

	fixture.controller.sync()

	require.True(t, proposer.Stopped(), "the view was not aborted before the new view was saved")
	require.True(t, fixture.state.savedAfterAbort, "the new view was saved before the view stopped")
	require.Len(t, fixture.state.SavedMessages, 1)
	require.NotNil(t, fixture.state.SavedMessages[0].GetNewView())
	require.Equal(t, []bool{false}, fixture.state.TruncateTo)
}

// TestSyncHoldsViewTransitionLockUntilTheNewViewIsSaved covers the read side
// of the record ordering: while sync() is between the abort and the new view
// record, changeView must not start another view.
func TestSyncHoldsViewTransitionLockUntilTheNewViewIsSaved(t *testing.T) {
	const (
		controllerSeq  = uint64(10)
		controllerView = uint64(5)
		syncSeq        = uint64(12)
		syncView       = uint64(6)
	)

	fixture := newSyncFixture(t, controllerSeq, controllerView, syncSeq, syncView)
	proposer := &View{Number: controllerView, abortChan: make(chan struct{})}
	fixture.controller.currView = proposer
	fixture.state.proposer = proposer

	saveStarted := make(chan struct{})
	saveRelease := make(chan struct{})
	fixture.state.beforeSave = func() {
		close(saveStarted)
		<-saveRelease
	}

	synced := make(chan struct{})
	go func() {
		fixture.controller.sync()
		close(synced)
	}()

	select {
	case <-saveStarted:
	case <-time.After(time.Second):
		t.Fatal("sync did not reach the new view record")
	}

	changed := make(chan struct{})
	go func() {
		fixture.controller.changeView(0, 0, 0)
		close(changed)
	}()

	select {
	case <-changed:
		t.Fatal("changeView returned while sync held the view transition lock")
	case <-time.After(50 * time.Millisecond):
	}

	close(saveRelease)

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("changeView did not continue after sync saved the new view")
	}

	select {
	case <-synced:
	case <-time.After(time.Second):
		t.Fatal("sync did not finish")
	}
}

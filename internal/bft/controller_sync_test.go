// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	proposer        *abortRecordingProposer
	savedAfterAbort bool
}

func (s *syncState) Save(msg *protos.SavedMessage, truncateTo bool) error {
	if s.proposer != nil && !s.proposer.aborted {
		s.savedAfterAbort = false
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

// newQuietLogger keeps the test output readable.
func newQuietLogger() api.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}

type abortRecordingProposer struct {
	aborted   bool
	abortChan chan struct{}
}

func (p *abortRecordingProposer) Propose(types.Proposal) {}
func (p *abortRecordingProposer) Start()                 {}

func (p *abortRecordingProposer) Abort() {
	p.aborted = true
	if p.abortChan != nil {
		close(p.abortChan)
	}
}

func (p *abortRecordingProposer) Stopped() bool              { return false }
func (p *abortRecordingProposer) AbortChan() <-chan struct{} { return p.abortChan }
func (p *abortRecordingProposer) GetLeaderID() uint64        { return 0 }
func (p *abortRecordingProposer) GetMetadata() []byte        { return nil }
func (p *abortRecordingProposer) HandleMessage(uint64, *protos.Message) {
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
	proposer := &abortRecordingProposer{}
	fixture.controller.currView = proposer
	fixture.state.proposer = proposer

	fixture.controller.sync()

	require.True(t, proposer.aborted, "the view was not aborted before the new view was saved")
	require.True(t, fixture.state.savedAfterAbort, "the new view was saved before the view stopped")
	require.Len(t, fixture.state.SavedMessages, 1)
	require.NotNil(t, fixture.state.SavedMessages[0].GetNewView())
	require.Equal(t, []bool{false}, fixture.state.TruncateTo)
}

// TestDecideDoesNotDropDecisionOnAbortedView covers the hand-off between the view
// and the controller: a view that is being aborted must not stay blocked on a full
// decision channel, but the committed decision must still reach the controller.
func TestDecideDoesNotDropDecisionOnAbortedView(t *testing.T) {
	proposer := &abortRecordingProposer{abortChan: make(chan struct{})}
	controller := &Controller{
		currView:     proposer,
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

	close(proposer.abortChan)

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

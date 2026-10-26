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

// stateTransferResponder answers a state transfer request with the same view
// and sequence from two nodes, which is enough for the collector quorum.
type stateTransferResponder struct {
	stubComm
	collector *StateCollector
	view      uint64
	seq       uint64
}

func (c *stateTransferResponder) SendConsensus(targetID uint64, m *protos.Message) {
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

// TestSyncSavesNewViewWithoutTruncating checks the sync call site: the record
// of the new view is saved with truncate=false in this change.
func TestSyncSavesNewViewWithoutTruncating(t *testing.T) {
	const (
		controllerSeq  = uint64(10)
		controllerView = uint64(3)
	)

	log := zap.NewNop().Sugar()

	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{
		Metadata: MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence:  controllerSeq,
			ViewId:          controllerView,
			DecisionsInView: 0,
		}),
	}, []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}})

	collector := &StateCollector{
		SelfID:         1,
		N:              4,
		Logger:         log,
		CollectTimeout: time.Second,
	}
	collector.Start()
	defer collector.Stop()

	state := &StateRecorder{}
	c := &Controller{
		ID:        1,
		N:         4,
		Logger:    log,
		NodesList: []uint64{1, 2, 3, 4},
		Comm: &stateTransferResponder{
			collector: collector,
			view:      controllerView + 1,
			seq:       controllerSeq + 1,
		},
		Synchronizer: &stubSynchronizer{response: types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence: controllerSeq,
						ViewId:         controllerView,
					}),
				},
			},
		}},
		Checkpoint:          checkpoint,
		Collector:           collector,
		InFlight:            &InFlightData{},
		ViewChanger:         &ViewChanger{informChan: make(chan uint64, 1)},
		State:               state,
		currViewNumber:      controllerView,
		currDecisionsInView: 1,
	}
	c.syncChan = make(chan struct{}, 1)

	viewNum, seq, decisions := c.sync()

	require.Equal(t, controllerView+1, viewNum)
	require.Equal(t, controllerSeq+1, seq)
	require.Equal(t, uint64(0), decisions)
	require.Len(t, state.SavedMessages, 1)
	require.NotNil(t, state.SavedMessages[0].GetNewView())
	require.Equal(t, []bool{false}, state.TruncateTo,
		"a new view saved after sync must not truncate the WAL in this change")
}

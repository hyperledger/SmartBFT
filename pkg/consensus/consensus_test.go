// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	algorithm "github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	bft "github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestComplainDoesNotBlockDuringReconfiguration covers the shutdown deadlock:
// a reconfiguration holds consensusLock and waits for the views to stop, while
// a view calls Complain and waits for that lock.
func TestComplainDoesNotBlockDuringReconfiguration(t *testing.T) {
	consensus := &Consensus{viewChanger: &algorithm.ViewChanger{}}

	consensus.consensusLock.Lock()
	defer consensus.consensusLock.Unlock()

	done := make(chan struct{})
	go func() {
		consensus.Complain(1, true)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Complain blocked while a reconfiguration held the lock")
	}
}

func TestSetViewAndSeqUsesTheTrailingProposalView(t *testing.T) {
	logger := zap.NewNop().Sugar()

	for _, testCase := range []struct {
		description       string
		proposalView      uint64
		proposalDecisions uint64
		carriedTo         uint64 // the view the proposal was carried into, if not 0
		expectedView      uint64
		expectedDecisions uint64
	}{
		{
			description:       "newer view in the proposal record",
			proposalView:      1,
			proposalDecisions: 5,
			expectedView:      1,
			expectedDecisions: 5,
		},
		{
			description:       "same view as the application metadata",
			proposalView:      0,
			proposalDecisions: 5,
			expectedView:      0,
			expectedDecisions: 3,
		},
		{
			description:       "proposal carried into a later view",
			proposalView:      1,
			proposalDecisions: 5,
			carriedTo:         2,
			expectedView:      2,
			expectedDecisions: 0,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			prePrepareView := testCase.proposalView
			if testCase.carriedTo != 0 {
				prePrepareView = testCase.carriedTo
			}
			proposal := &smartbftprotos.SavedMessage{
				Content: &smartbftprotos.SavedMessage_ProposedRecord{
					ProposedRecord: &smartbftprotos.ProposedRecord{
						PrePrepare: &smartbftprotos.PrePrepare{
							Seq:  11,
							View: prePrepareView,
							Proposal: &smartbftprotos.Proposal{
								Metadata: algorithm.MarshalOrPanic(&smartbftprotos.ViewMetadata{
									ViewId:          testCase.proposalView,
									LatestSequence:  11,
									DecisionsInView: testCase.proposalDecisions,
								}),
							},
						},
					},
				},
			}

			consensus := &Consensus{
				Logger: logger,
				state: &algorithm.PersistedState{
					Entries: [][]byte{algorithm.MarshalOrPanic(proposal)},
					Logger:  logger,
				},
			}

			// The application metadata is at sequence 10 with 2 decisions, so
			// setViewAndSeq starts with 3 decisions and the view from metadata.
			view, seq, dec := consensus.setViewAndSeq(0, 10, 2)
			require.Equal(t, testCase.expectedView, view)
			require.Equal(t, uint64(10), seq)
			require.Equal(t, testCase.expectedDecisions, dec)
		})
	}
}

// TestSetViewAndSeqWithACarriedProposal makes sure that a node restarts in the view a proposal was carried into, which
// keeps the metadata of the view it was proposed in, whether the proposal is still in flight or already decided. The
// proposal doesn't count as a decision in that view, so the node restarts with none.
func TestSetViewAndSeqWithACarriedProposal(t *testing.T) {
	logger := zap.NewNop().Sugar()
	// proposed at sequence 11 in view 5, after 5 decisions in it, and carried into view 6
	carried := &smartbftprotos.SavedMessage{
		Content: &smartbftprotos.SavedMessage_ProposedRecord{
			ProposedRecord: &smartbftprotos.ProposedRecord{
				PrePrepare: &smartbftprotos.PrePrepare{
					Seq:  11,
					View: 6,
					Proposal: &smartbftprotos.Proposal{
						Metadata: algorithm.MarshalOrPanic(&smartbftprotos.ViewMetadata{
							ViewId:          5,
							LatestSequence:  11,
							DecisionsInView: 5,
						}),
					},
				},
			},
		},
	}
	commit := &smartbftprotos.SavedMessage{
		Content: &smartbftprotos.SavedMessage_Commit{
			Commit: &smartbftprotos.Message{
				Content: &smartbftprotos.Message_Commit{Commit: &smartbftprotos.Commit{Seq: 11, View: 6}},
			},
		},
	}
	newView := &smartbftprotos.SavedMessage{
		Content: &smartbftprotos.SavedMessage_NewView{
			NewView: &smartbftprotos.ViewMetadata{ViewId: 6, LatestSequence: 10},
		},
	}

	for _, test := range []struct {
		description       string
		WALContent        []*smartbftprotos.SavedMessage
		seq, decisions    uint64 // of the last decision of the application, in view 5
		expectedDecisions uint64
	}{
		{
			description:       "in flight",
			WALContent:        []*smartbftprotos.SavedMessage{carried},
			seq:               10,
			decisions:         4,
			expectedDecisions: 0,
		},
		{
			description:       "decided",
			WALContent:        []*smartbftprotos.SavedMessage{carried, commit},
			seq:               11,
			decisions:         5,
			expectedDecisions: 0,
		},
		{
			description:       "decided, after a new view record that the write ahead log kept",
			WALContent:        []*smartbftprotos.SavedMessage{newView, carried, commit},
			seq:               11,
			decisions:         5,
			expectedDecisions: 0,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			entries := make([][]byte, 0, len(test.WALContent))
			for _, record := range test.WALContent {
				entries = append(entries, algorithm.MarshalOrPanic(record))
			}
			consensus := &Consensus{
				Logger: logger,
				state:  &algorithm.PersistedState{Entries: entries, Logger: logger},
			}

			view, seq, dec := consensus.setViewAndSeq(5, test.seq, test.decisions)
			require.Equal(t, uint64(6), view)
			require.Equal(t, test.seq, seq)
			require.Equal(t, test.expectedDecisions, dec)
		})
	}
}

// TestSetViewAndSeqDoesNotStartALeftView makes sure that a node whose write ahead log ends with a view change record
// starts in the view it left, restores the view change, and that the controller then doesn't run that view.
func TestSetViewAndSeqDoesNotStartALeftView(t *testing.T) {
	logger := zap.NewNop().Sugar()

	viewChange := &smartbftprotos.SavedMessage{
		Content: &smartbftprotos.SavedMessage_ViewChange{
			ViewChange: &smartbftprotos.ViewChange{NextView: 3}, // the view the node left
		},
	}
	// the view is built, but not started
	proposer := &mocks.Proposer{}
	proposer.On("Start").Maybe()
	proposer.On("Abort").Maybe() // if it was started, the controller aborts it when it stops
	builder := &mocks.ProposerBuilder{}
	builder.On("NewProposer", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(proposer, algorithm.Phase(algorithm.COMMITTED))
	leaderMonitor := &mocks.LeaderMonitor{}
	leaderMonitor.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything)
	leaderMonitor.On("Close")
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	var started sync.WaitGroup
	started.Add(1)

	consensus := &Consensus{
		Logger:      logger,
		viewChanger: &algorithm.ViewChanger{},
		controller: &algorithm.Controller{
			Logger:          logger,
			N:               4,
			NodesList:       []uint64{1, 2, 3, 4},
			Checkpoint:      &types.Checkpoint{},
			ProposerBuilder: builder,
			LeaderMonitor:   leaderMonitor,
			Batcher:         batcher,
			RequestPool:     pool,
			ViewSequences:   &atomic.Value{},
			StartedWG:       &started,
		},
		state: &algorithm.PersistedState{
			Entries: [][]byte{algorithm.MarshalOrPanic(viewChange)},
			Logger:  logger,
		},
	}

	view, seq, dec := consensus.setViewAndSeq(2, 10, 2)
	require.Equal(t, uint64(3), view)
	require.NotNil(t, consensus.viewChanger.Restore, "the view change is not restored")

	consensus.controller.Start(view, seq+1, dec, 1, false)
	defer consensus.controller.Stop()
	builder.AssertNumberOfCalls(t, "NewProposer", 1)
	proposer.AssertNotCalled(t, "Start")
	leaderMonitor.AssertCalled(t, "ChangeRole", algorithm.Follower, uint64(3), mock.Anything)
}

// TestNewProposerWiresTheView makes sure that the views get the in-flight proposal that the persisted state and the
// view changer use, so that a view knows whether a proposal it gets is already prepared, and the proposal carried into
// them.
func TestNewProposerWiresTheView(t *testing.T) {
	logger := zap.NewNop().Sugar()
	inFlight := &algorithm.InFlightData{}
	consensus := &Consensus{
		Logger:     logger,
		Metrics:    bft.NewMetrics(&disabled.Provider{}),
		inFlight:   inFlight,
		checkpoint: &types.Checkpoint{},
		state:      &algorithm.PersistedState{InFlightProposal: inFlight, Logger: logger},
		controller: &algorithm.Controller{ViewSequences: &atomic.Value{}},
	}

	carried := &types.Proposal{Payload: []byte{1}}
	view, _ := consensus.proposalMaker().NewProposer(1, 1, 0, 0, 3, carried)
	require.Same(t, inFlight, view.(*algorithm.View).InFlight)
	require.Same(t, carried, view.(*algorithm.View).Carried)
}

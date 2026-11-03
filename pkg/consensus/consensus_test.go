// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"testing"
	"time"

	algorithm "github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
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
	} {
		t.Run(testCase.description, func(t *testing.T) {
			proposal := &smartbftprotos.SavedMessage{
				Content: &smartbftprotos.SavedMessage_ProposedRecord{
					ProposedRecord: &smartbftprotos.ProposedRecord{
						PrePrepare: &smartbftprotos.PrePrepare{
							Seq:  11,
							View: testCase.proposalView,
							Proposal: &smartbftprotos.Proposal{
								Metadata: algorithm.MarshalOrPanic(&smartbftprotos.ViewMetadata{
									ViewId:          testCase.proposalView,
									LatestSequence:  10,
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

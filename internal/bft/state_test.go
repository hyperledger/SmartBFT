// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestStateRestore(t *testing.T) {
	prePrepare := &protos.PrePrepare{
		Proposal: &protos.Proposal{
			Header:  []byte{1},
			Payload: []byte{1},
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				DecisionsInView: 0,
				LatestSequence:  0,
				ViewId:          1,
			}),
			VerificationSequence: 100,
		},
		Seq:  200,
		View: 300,
	}

	expectedInFlightProposal := &types.Proposal{
		VerificationSequence: int64(prePrepare.Proposal.VerificationSequence),
		Metadata:             prePrepare.Proposal.Metadata,
		Payload:              prePrepare.Proposal.Payload,
		Header:               prePrepare.Proposal.Header,
	}

	proposedRecord := &protos.SavedMessage{
		Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: prePrepare,
				Prepare: &protos.Prepare{
					Seq:  200,
					View: 300,
				},
			},
		},
	}

	preparedProof := &protos.SavedMessage{
		Content: &protos.SavedMessage_Commit{
			Commit: &protos.Message{
				Content: &protos.Message_Commit{
					Commit: &protos.Commit{
						Seq:  200,
						View: 300,
						Signature: &protos.Signature{
							Signer: 11,
						},
					},
				},
			},
		},
	}

	for _, testCase := range []struct {
		proposalSeqViewInitializedWith uint64
		description                    string
		WALContent                     [][]byte
		expectedError                  string
		expectedPhase                  bft.Phase
		expectedViewNumber             uint64
		expectedProposalSeq            uint64
		expectedInFlightProposal       *types.Proposal
		expectedInFlightPrepared       bool
	}{
		{
			description:        "empty",
			expectedViewNumber: 300,
		},
		{
			description: "malformed record",
			WALContent:  [][]byte{{1, 2, 3}},
			expectedError: "failed unmarshaling last entry from WAL:" +
				" proto:",
		},
		{
			description:   "unidentified record",
			WALContent:    [][]byte{nil},
			expectedError: "unrecognized record: ",
		},
		{
			description:              "proposed",
			expectedPhase:            bft.PROPOSED,
			expectedViewNumber:       300,
			expectedProposalSeq:      200,
			WALContent:               [][]byte{bft.MarshalOrPanic(proposedRecord)},
			expectedInFlightProposal: expectedInFlightProposal,
		},
		{
			description:   "commit persisted but pre-prepare nowhere to be found",
			expectedPhase: bft.PREPARED,
			WALContent: [][]byte{
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "last message is a commit, but expected to also have a matching pre-prepare",
		},
		{
			description:         "prepared but not committed",
			expectedPhase:       bft.PREPARED,
			expectedViewNumber:  300,
			expectedProposalSeq: 200,
			// Metadata holds sequence 199, WAL has sequence 200
			proposalSeqViewInitializedWith: 200,
			WALContent: [][]byte{
				bft.MarshalOrPanic(proposedRecord),
				bft.MarshalOrPanic(preparedProof),
			},
			expectedInFlightProposal: expectedInFlightProposal,
			expectedInFlightPrepared: true,
		},
		{
			description:         "prepared and committed",
			expectedPhase:       bft.COMMITTED,
			expectedViewNumber:  300,
			expectedProposalSeq: 201,
			// Metadata holds sequence 200 which is the same as WAL
			proposalSeqViewInitializedWith: 201,
			WALContent: [][]byte{
				bft.MarshalOrPanic(proposedRecord),
				bft.MarshalOrPanic(preparedProof),
			},
		},
		{
			description:   "WAL out of sync",
			expectedPhase: bft.COMMITTED,
			// Metadata holds sequence 198 but WAL has sequence 200
			proposalSeqViewInitializedWith: 199,
			WALContent: [][]byte{
				bft.MarshalOrPanic(proposedRecord),
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "last proposal sequence persisted into WAL is 200" +
				" which is greater than last committed sequence is 199",
		},
		{
			description:   "malformed prepared but valid commit",
			expectedPhase: bft.COMMITTED,
			WALContent: [][]byte{
				{1, 2, 3},
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "failed unmarshaling last entry from WAL: " +
				"proto:",
		},
		{
			description:   "malformed prepared but empty commit",
			expectedPhase: bft.COMMITTED,
			WALContent: [][]byte{
				bft.MarshalOrPanic(&protos.SavedMessage{Content: nil}),
				bft.MarshalOrPanic(preparedProof),
			},
			expectedError: "expected second last message to be a pre-prepare, but got '' instead",
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			log := basicLog.Sugar()

			state := &bft.PersistedState{
				Entries:          testCase.WALContent,
				Logger:           log,
				InFlightProposal: &bft.InFlightData{},
			}

			verifier := &mocks.VerifierMock{}
			verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
			view := &bft.View{
				Number:           300,
				ProposalSequence: testCase.proposalSeqViewInitializedWith,
				MetricsView:      api.NewMetricsView(&disabled.Provider{}),
				Verifier:         verifier,
			}

			err = state.Restore(view)

			if testCase.expectedError == "" {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expectedPhase, view.Phase)
				assert.Equal(t, testCase.expectedViewNumber, view.Number)
				assert.Equal(t, testCase.expectedProposalSeq, view.ProposalSequence)
				assert.Equal(t, testCase.expectedInFlightProposal, state.InFlightProposal.InFlightProposal())
				assert.Equal(t, testCase.expectedInFlightPrepared, state.InFlightProposal.IsInFlightPrepared())
			} else {
				assert.Contains(t, err.Error(), testCase.expectedError)
			}
		})
	}
}

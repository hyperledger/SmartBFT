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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestStateRestore(t *testing.T) {
	proposedRecord := newProposalRecord(200, 300, &protos.ViewMetadata{
		DecisionsInView: 0,
		LatestSequence:  0,
		ViewId:          1,
	})
	prePrepare := proposedRecord.GetProposedRecord().GetPrePrepare()

	expectedInFlightProposal := &types.Proposal{
		VerificationSequence: int64(prePrepare.GetProposal().GetVerificationSequence()),
		Metadata:             prePrepare.GetProposal().GetMetadata(),
		Payload:              prePrepare.GetProposal().GetPayload(),
		Header:               prePrepare.GetProposal().GetHeader(),
	}

	preparedProof := newCommitRecord(200, 300)

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
			require.NoError(t, err)
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
				require.NoError(t, err)
				require.Equal(t, testCase.expectedPhase, view.Phase)
				require.Equal(t, testCase.expectedViewNumber, view.Number)
				require.Equal(t, testCase.expectedProposalSeq, view.ProposalSequence)
				require.Equal(t, testCase.expectedInFlightProposal, state.InFlightProposal.InFlightProposal())
				require.Equal(t, testCase.expectedInFlightPrepared, state.InFlightProposal.IsInFlightPrepared())
			} else {
				require.Contains(t, err.Error(), testCase.expectedError)
			}
		})
	}
}

func TestStateLoadIfApplicable(t *testing.T) {
	newView := &protos.SavedMessage{
		Content: &protos.SavedMessage_NewView{
			NewView: &protos.ViewMetadata{
				ViewId:         5,
				LatestSequence: 10,
			},
		},
	}

	viewChange := &protos.ViewChange{
		NextView: 6,
		Reason:   "leader is unresponsive",
	}
	savedViewChange := &protos.SavedMessage{
		Content: &protos.SavedMessage_ViewChange{
			ViewChange: viewChange,
		},
	}

	proposedRecord := &protos.SavedMessage{
		Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: &protos.PrePrepare{Seq: 11, View: 5},
			},
		},
	}

	for _, testCase := range []struct {
		description        string
		WALContent         [][]byte
		expectedError      string
		expectedViewAndSeq *types.ViewAndSeq
		expectedViewChange *protos.ViewChange
	}{
		{
			description: "empty",
		},
		{
			description:   "malformed record",
			WALContent:    [][]byte{{1, 2, 3}},
			expectedError: "failed unmarshaling last entry from WAL: proto:",
		},
		{
			description: "last record is a proposal",
			WALContent:  [][]byte{bft.MarshalOrPanic(newView), bft.MarshalOrPanic(proposedRecord)},
		},
		{
			description:        "last record is a new view",
			WALContent:         [][]byte{bft.MarshalOrPanic(savedViewChange), bft.MarshalOrPanic(newView)},
			expectedViewAndSeq: &types.ViewAndSeq{View: 5, Seq: 10},
		},
		{
			description:        "last record is a view change",
			WALContent:         [][]byte{bft.MarshalOrPanic(newView), bft.MarshalOrPanic(savedViewChange)},
			expectedViewChange: viewChange,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			require.NoError(t, err)

			state := &bft.PersistedState{
				Entries: testCase.WALContent,
				Logger:  basicLog.Sugar(),
			}

			viewAndSeq, err := state.LoadNewViewIfApplicable()
			if testCase.expectedError != "" {
				require.ErrorContains(t, err, testCase.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, testCase.expectedViewAndSeq, viewAndSeq)

			vc, err := state.LoadViewChangeIfApplicable()
			if testCase.expectedError != "" {
				require.ErrorContains(t, err, testCase.expectedError)
			} else {
				require.NoError(t, err)
			}
			if testCase.expectedViewChange == nil {
				require.Nil(t, vc)
			} else {
				require.True(t, proto.Equal(testCase.expectedViewChange, vc))
			}
		})
	}
}

// recordingWAL stands in for the write ahead log at the api.WriteAheadLog seam
// and keeps, in order, every appended record together with its truncation flag.
type recordingWAL struct {
	appended   [][]byte
	truncateTo []bool
}

func (w *recordingWAL) Append(entry []byte, truncateTo bool) error {
	w.appended = append(w.appended, entry)
	w.truncateTo = append(w.truncateTo, truncateTo)

	return nil
}

func newViewRecord(viewID, latestSequence uint64) *protos.SavedMessage {
	return &protos.SavedMessage{
		Content: &protos.SavedMessage_NewView{
			NewView: &protos.ViewMetadata{
				ViewId:         viewID,
				LatestSequence: latestSequence,
			},
		},
	}
}

func newProposalRecord(seq, view uint64, metadata *protos.ViewMetadata) *protos.SavedMessage {
	return &protos.SavedMessage{
		Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: &protos.PrePrepare{
					Proposal: &protos.Proposal{
						Header:               []byte{1},
						Payload:              []byte{1},
						Metadata:             bft.MarshalOrPanic(metadata),
						VerificationSequence: 100,
					},
					Seq:  seq,
					View: view,
				},
				Prepare: &protos.Prepare{
					Seq:  seq,
					View: view,
				},
			},
		},
	}
}

func newCommitRecord(seq, view uint64) *protos.SavedMessage {
	return &protos.SavedMessage{
		Content: &protos.SavedMessage_Commit{
			Commit: &protos.Message{
				Content: &protos.Message_Commit{
					Commit: &protos.Commit{
						Seq:  seq,
						View: view,
						Signature: &protos.Signature{
							Signer: 11,
						},
					},
				},
			},
		},
	}
}

// The caller decides where the WAL may be truncated. Save must persist that
// decision as given, together with the record.
func TestSaveUsesTheGivenTruncationFlag(t *testing.T) {
	for _, testCase := range []struct {
		description   string
		recordsToSave []*protos.SavedMessage
		truncateTo    []bool
	}{
		{
			description:   "a proposal begins the pair, so it is a truncation point",
			recordsToSave: []*protos.SavedMessage{newProposalRecord(200, 300, &protos.ViewMetadata{ViewId: 1})},
			truncateTo:    []bool{true},
		},
		{
			description: "a commit right after the proposal keeps the pair",
			recordsToSave: []*protos.SavedMessage{
				newProposalRecord(200, 300, &protos.ViewMetadata{ViewId: 1}),
				newCommitRecord(200, 300),
			},
			truncateTo: []bool{true, false},
		},
		{
			description:   "a new view is saved with the flag given by the caller",
			recordsToSave: []*protos.SavedMessage{newViewRecord(7, 4)},
			truncateTo:    []bool{false},
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			require.NoError(t, err)

			writeAheadLog := &recordingWAL{}
			state := &bft.PersistedState{
				InFlightProposal: &bft.InFlightData{},
				Logger:           basicLog.Sugar(),
				WAL:              writeAheadLog,
			}

			for i, record := range testCase.recordsToSave {
				require.NoError(t, state.Save(record, testCase.truncateTo[i]))
			}

			require.Equal(t, testCase.truncateTo, writeAheadLog.truncateTo)

			require.Len(t, writeAheadLog.appended, len(testCase.recordsToSave))
			for i, record := range testCase.recordsToSave {
				appended := &protos.SavedMessage{}
				require.NoError(t, proto.Unmarshal(writeAheadLog.appended[i], appended))
				require.True(t, proto.Equal(record, appended), "record %d was not appended as saved", i)
			}
		})
	}
}

// A commit is never saved by the view with truncate=true: its pre-prepare has to
// survive next to it. The flag is not read from the shared in-flight state, so it
// cannot change while Save is running.
func TestSaveCommitUsesTheGivenTruncationFlag(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)

	inFlight := &bft.InFlightData{}
	inFlight.StoreProposal(types.Proposal{VerificationSequence: 42})

	writeAheadLog := &recordingWAL{}
	state := &bft.PersistedState{
		InFlightProposal: inFlight,
		Logger:           basicLog.Sugar(),
		WAL:              writeAheadLog,
	}

	require.NoError(t, state.Save(newCommitRecord(200, 300), false))

	require.Equal(t, []bool{false}, writeAheadLog.truncateTo, "a commit must never be a truncation point")
	require.True(t, inFlight.IsInFlightPrepared(), "saving a commit must not clear the proposal in flight")
}

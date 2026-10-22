// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"errors"
	"fmt"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type StateRecorder struct {
	SavedMessages []*protos.SavedMessage
}

func (sr *StateRecorder) Save(message *protos.SavedMessage) error {
	sr.SavedMessages = append(sr.SavedMessages, message)
	return nil
}

func (*StateRecorder) Restore(_ *View) error {
	panic("should not be used")
}

type PersistedState struct {
	InFlightProposal *InFlightData
	Entries          [][]byte
	Logger           api.Logger
	WAL              api.WriteAheadLog
}

func (ps *PersistedState) Save(msgToSave *protos.SavedMessage) error {
	if proposed := msgToSave.GetProposedRecord(); proposed != nil {
		ps.storeProposal(proposed)
	}
	if prepared := msgToSave.GetCommit(); prepared != nil {
		ps.storePrepared(prepared)
	}

	b, err := proto.Marshal(msgToSave)
	if err != nil {
		ps.Logger.Panicf("Failed marshaling message: %v", err)
	}
	// It is only safe to truncate if we either:
	//
	// 1) Process a pre-prepare, because it means we safely persisted the
	// previous proposal and have a stable checkpoint.
	// 2) Acquired a new view message which contains 2f+1 attestations
	//    of the cluster agreeing to a new view configuration.
	newProposal := msgToSave.GetProposedRecord() != nil
	// TODO: handle view message here as well, and add "|| finalizedView" to truncate flag
	return ps.WAL.Append(b, newProposal)
}

func (ps *PersistedState) storeProposal(proposed *protos.ProposedRecord) {
	proposal := proposed.GetPrePrepare().GetProposal()
	proposalToStore := types.Proposal{
		VerificationSequence: int64(proposal.GetVerificationSequence()),
		Header:               proposal.GetHeader(),
		Payload:              proposal.GetPayload(),
		Metadata:             proposal.GetMetadata(),
	}
	ps.InFlightProposal.StoreProposal(proposalToStore)
}

func (ps *PersistedState) storePrepared(commitMsg *protos.Message) {
	cmt := commitMsg.GetCommit()
	ps.InFlightProposal.StorePrepares(cmt.GetView(), cmt.GetSeq())
}

func (ps *PersistedState) LoadNewViewIfApplicable() (*types.ViewAndSeq, error) {
	entries := ps.Entries
	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil, nil
	}
	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &protos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return nil, fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}
	if newViewMsg := lastPersistedMessage.GetNewView(); newViewMsg != nil {
		ps.Logger.Infof("last entry in WAL is a newView record")
		return &types.ViewAndSeq{View: newViewMsg.GetViewId(), Seq: newViewMsg.GetLatestSequence()}, nil
	}
	return nil, nil
}

func (ps *PersistedState) LoadViewChangeIfApplicable() (*protos.ViewChange, error) {
	entries := ps.Entries
	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil, nil
	}
	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &protos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return nil, fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}
	if viewChangeMsg := lastPersistedMessage.GetViewChange(); viewChangeMsg != nil {
		ps.Logger.Infof("last entry in WAL is a viewChange message")
		return viewChangeMsg, nil
	}
	return nil, nil
}

func (ps *PersistedState) Restore(v *View) error {
	// Unless we conclude otherwise, we're in a COMMITTED state
	v.Phase = COMMITTED

	entries := ps.Entries
	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil
	}

	ps.Logger.Infof("WAL contains %d entries", len(entries))

	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &protos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}

	if proposed := lastPersistedMessage.GetProposedRecord(); proposed != nil {
		return ps.recoverProposed(proposed, v)
	}

	if commitMsg := lastPersistedMessage.GetCommit(); commitMsg != nil {
		return ps.recoverPrepared(commitMsg, v, entries)
	}

	if newViewMsg := lastPersistedMessage.GetNewView(); newViewMsg != nil {
		ps.Logger.Infof("last entry in WAL is a newView record")
		return nil
	}

	if viewChangeMsg := lastPersistedMessage.GetViewChange(); viewChangeMsg != nil {
		ps.Logger.Infof("last entry in WAL is a viewChange message")
		return nil
	}

	return fmt.Errorf("unrecognized record: %v", lastPersistedMessage)
}

func (ps *PersistedState) recoverProposed(lastPersistedMessage *protos.ProposedRecord, v *View) error {
	prop := lastPersistedMessage.GetPrePrepare().GetProposal()
	v.inFlightProposal = &types.Proposal{
		VerificationSequence: int64(prop.GetVerificationSequence()),
		Metadata:             prop.GetMetadata(),
		Payload:              prop.GetPayload(),
		Header:               prop.GetHeader(),
	}
	restoreInFlightRequests(v)
	ps.storeProposal(lastPersistedMessage)
	// Reconstruct the prepare message we shall next broadcast
	// after the recovery.
	prp := lastPersistedMessage.GetPrePrepare()
	v.lastBroadcastSent = &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: lastPersistedMessage.GetPrepare(),
		},
	}
	v.Phase = PROPOSED
	v.Number = prp.GetView()
	v.ProposalSequence = prp.GetSeq()
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prop.GetMetadata(), md); err != nil {
		ps.Logger.Panicf("Failed to unmarshal proposal metadata, error: %v", err)
	}
	v.DecisionsInView = md.GetDecisionsInView()
	ps.Logger.Infof("Restored proposal with sequence %d", lastPersistedMessage.GetPrePrepare().GetSeq())
	return nil
}

func (ps *PersistedState) recoverPrepared(lastPersistedMessage *protos.Message, v *View, entries [][]byte) error {
	// Last entry is a commit, so we should have not pruned the previous pre-prepare
	if len(entries) < 2 {
		return errors.New("last message is a commit, but expected to also have a matching pre-prepare")
	}
	prePrepareMsg := &protos.SavedMessage{}
	if err := proto.Unmarshal(entries[len(entries)-2], prePrepareMsg); err != nil {
		ps.Logger.Errorf("Failed unmarshaling second last entry from WAL: %v", err)
		return fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}

	prePrepareFromWAL := prePrepareMsg.GetProposedRecord().GetPrePrepare()

	if prePrepareFromWAL == nil {
		return fmt.Errorf("expected second last message to be a pre-prepare, but got '%v' instead", prePrepareMsg)
	}

	if v.ProposalSequence < prePrepareFromWAL.GetSeq() {
		err := fmt.Errorf("last proposal sequence persisted into WAL is %d which is greater than last committed sequence is %d", prePrepareFromWAL.GetSeq(), v.ProposalSequence)
		ps.Logger.Errorf("Failed recovery: %s", err)
		return err
	}

	// Check if the WAL's last sequence has been persisted into the application layer.
	if v.ProposalSequence > prePrepareFromWAL.GetSeq() {
		ps.Logger.Infof("Last proposal with sequence %d has been safely committed", v.ProposalSequence)
		return nil
	}

	// Else, v.ProposalSequence == prePrepareFromWAL.Seq

	prop := prePrepareFromWAL.GetProposal()
	v.inFlightProposal = &types.Proposal{
		VerificationSequence: int64(prop.GetVerificationSequence()),
		Metadata:             prop.GetMetadata(),
		Payload:              prop.GetPayload(),
		Header:               prop.GetHeader(),
	}
	restoreInFlightRequests(v)
	ps.storeProposal(prePrepareMsg.GetProposedRecord())
	ps.storePrepared(lastPersistedMessage)

	// Reconstruct the commit message we shall next broadcast
	// after the recovery.
	v.lastBroadcastSent = lastPersistedMessage
	v.Phase = PREPARED
	v.Number = prePrepareFromWAL.GetView()
	v.ProposalSequence = prePrepareFromWAL.GetSeq()
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prop.GetMetadata(), md); err != nil {
		ps.Logger.Panicf("Failed to unmarshal proposal metadata, error: %v", err)
	}
	v.DecisionsInView = md.GetDecisionsInView()

	// Restore signature
	signatureInLastSentCommit := v.lastBroadcastSent.GetCommit().GetSignature()
	v.myProposalSig = &types.Signature{
		ID:    signatureInLastSentCommit.GetSigner(),
		Msg:   signatureInLastSentCommit.GetMsg(),
		Value: signatureInLastSentCommit.GetValue(),
	}

	ps.Logger.Infof("Restored proposal with sequence %d", prePrepareFromWAL.GetSeq())
	return nil
}

// restoreInFlightRequests sets what a view sets when it accepts a proposal and that is not persisted in the WAL,
// so that once the restored proposal is decided its requests are removed from the pool,
// and the time it took to process it is measured from the restore.
func restoreInFlightRequests(v *View) {
	v.inFlightRequests = v.Verifier.RequestsFromProposal(*v.inFlightProposal)
	v.beginPrePrepare = time.Now()
}

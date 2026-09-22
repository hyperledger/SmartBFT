// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"go.uber.org/zap"
)

type recordedComplaint struct {
	view     uint64
	stopView bool
}

type recordingFailureDetector struct {
	complaints chan recordedComplaint
}

func (r *recordingFailureDetector) Complain(view uint64, stopView bool) {
	r.complaints <- recordedComplaint{view: view, stopView: stopView}
}

type prepareTimeoutSigner struct {
	id uint64
}

func (s *prepareTimeoutSigner) Sign([]byte) []byte {
	return nil
}

func (s *prepareTimeoutSigner) SignProposal(_ types.Proposal, auxiliaryInput []byte) *types.Signature {
	return &types.Signature{ID: s.id, Msg: auxiliaryInput}
}

func newPrepareTimeoutView(timeout time.Duration) (*View, *recordingFailureDetector) {
	proposal := &types.Proposal{Payload: []byte("approved-qc-fixture")}
	failureDetector := &recordingFailureDetector{complaints: make(chan recordedComplaint, 1)}
	v := &View{
		SelfID:                       1,
		N:                            4,
		NodesList:                    []uint64{1, 2, 3, 4},
		LeaderID:                     1,
		Quorum:                       3,
		Number:                       7,
		ProposalSequence:             9,
		PrepareVoteCollectionTimeout: timeout,
		FailureDetector:              failureDetector,
		Logger:                       zap.NewNop().Sugar(),
		Signer:                       &prepareTimeoutSigner{id: 1},
		State:                        &StateRecorder{},
		inFlightProposal:             proposal,
		abortChan:                    make(chan struct{}),
	}
	v.setupVotes()
	return v, failureDetector
}

func prepareVote(v *View) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   v.Number,
				Seq:    v.ProposalSequence,
				Digest: v.inFlightProposal.Digest(),
			},
		},
	}
}

func TestPrepareQuorumContinuesWithoutWaitingForTimeout(t *testing.T) {
	v, failureDetector := newPrepareTimeoutView(5 * time.Minute)
	v.prepares.registerVote(2, prepareVote(v))
	v.prepares.registerVote(3, prepareVote(v))

	start := time.Now()
	if phase := v.processPrepares(); phase != PREPARED {
		t.Fatalf("expected PREPARED, got %s", phase.String())
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("prepare quorum waited for timeout: %s", elapsed)
	}
	select {
	case complaint := <-failureDetector.complaints:
		t.Fatalf("unexpected complaint: %+v", complaint)
	default:
	}
}

func TestPrepareVoteCollectionTimeoutComplainsAndAborts(t *testing.T) {
	const timeout = 25 * time.Millisecond
	v, failureDetector := newPrepareTimeoutView(timeout)

	start := time.Now()
	if phase := v.processPrepares(); phase != ABORT {
		t.Fatalf("expected ABORT, got %s", phase.String())
	}
	if elapsed := time.Since(start); elapsed < timeout {
		t.Fatalf("prepare timeout fired early after %s", elapsed)
	}

	select {
	case complaint := <-failureDetector.complaints:
		if complaint.view != v.Number || !complaint.stopView {
			t.Fatalf("unexpected complaint: %+v", complaint)
		}
	default:
		t.Fatal("expected a view-change complaint")
	}
}

func TestPrepareVoteCollectionStopsWhenViewIsAborted(t *testing.T) {
	v, failureDetector := newPrepareTimeoutView(5 * time.Minute)
	close(v.abortChan)

	if phase := v.processPrepares(); phase != ABORT {
		t.Fatalf("expected ABORT, got %s", phase.String())
	}
	select {
	case complaint := <-failureDetector.complaints:
		t.Fatalf("unexpected complaint after view abort: %+v", complaint)
	default:
	}
}

func TestPrepareBufferStateIsBoundToExactProposalAndRequest(t *testing.T) {
	const timeout = time.Second
	v, _ := newPrepareTimeoutView(timeout)
	requestInfo := types.RequestInfo{ClientID: "mwvn-ingress", ID: "approved-qc-1"}
	v.inFlightRequests = []types.RequestInfo{requestInfo}

	result := make(chan Phase, 1)
	go func() {
		result <- v.processPrepares()
	}()

	var buffer PrepareBufferInfo
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		var active bool
		buffer, active = v.PrepareBufferInfo(requestInfo)
		if active {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if buffer.Digest == "" {
		t.Fatal("active PREPARE buffer was not published")
	}
	if buffer.View != v.Number || buffer.Sequence != v.ProposalSequence || buffer.Digest != v.inFlightProposal.Digest() {
		t.Fatalf("buffer is not bound to the active proposal: %+v", buffer)
	}
	if !buffer.Deadline.After(time.Now()) {
		t.Fatalf("buffer deadline is not in the future: %s", buffer.Deadline)
	}
	if _, active := v.PrepareBufferInfo(types.RequestInfo{ClientID: requestInfo.ClientID, ID: "different-qc"}); active {
		t.Fatal("unrelated request matched the active PREPARE buffer")
	}

	v.prepares.registerVote(2, prepareVote(v))
	v.prepares.registerVote(3, prepareVote(v))
	select {
	case phase := <-result:
		if phase != PREPARED {
			t.Fatalf("expected PREPARED, got %s", phase.String())
		}
	case <-time.After(time.Second):
		t.Fatal("PREPARE quorum did not release the buffer")
	}
	if _, active := v.PrepareBufferInfo(requestInfo); active {
		t.Fatal("PREPARE buffer remained active after quorum")
	}
}

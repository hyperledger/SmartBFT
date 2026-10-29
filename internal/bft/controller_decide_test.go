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
)

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

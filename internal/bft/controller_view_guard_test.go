// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
)

type guardTestBatcher struct{}

func (guardTestBatcher) NextBatch() [][]byte { return nil }
func (guardTestBatcher) Close()              {}
func (guardTestBatcher) Closed() bool        { return false }
func (guardTestBatcher) Reset()              {}

type guardTestRequestPool struct{}

func (guardTestRequestPool) Prune(func([]byte) error) {}
func (guardTestRequestPool) Submit([]byte) error      { return nil }
func (guardTestRequestPool) Size() int                { return 0 }
func (guardTestRequestPool) NextRequests(int, uint64, bool) ([][]byte, bool) {
	return nil, false
}
func (guardTestRequestPool) RemoveRequest(types.RequestInfo) error { return nil }
func (guardTestRequestPool) StopTimers()                           {}
func (guardTestRequestPool) RestartTimers()                        {}
func (guardTestRequestPool) Close()                                {}

type guardTestLeaderMonitor struct{}

func (guardTestLeaderMonitor) ChangeRole(Role, uint64, uint64)                   {}
func (guardTestLeaderMonitor) ProcessMsg(uint64, *protos.Message)                {}
func (guardTestLeaderMonitor) InjectArtificialHeartbeat(uint64, *protos.Message) {}
func (guardTestLeaderMonitor) HeartbeatWasSent()                                 {}
func (guardTestLeaderMonitor) Close()                                            {}
func (guardTestLeaderMonitor) StopLeaderSendMsg()                                {}

type guardTestProposer struct {
	aborted      atomic.Bool
	abortStarted chan struct{}
	abortRelease chan struct{}
	once         sync.Once
}

func (p *guardTestProposer) Propose(types.Proposal) {}
func (p *guardTestProposer) Start()                 {}

func (p *guardTestProposer) Abort() {
	if p.abortStarted != nil {
		p.once.Do(func() {
			close(p.abortStarted)
		})
		<-p.abortRelease
	}

	p.aborted.Store(true)
}

func (p *guardTestProposer) Stopped() bool                         { return p.aborted.Load() }
func (p *guardTestProposer) AbortChan() <-chan struct{}            { return nil }
func (p *guardTestProposer) GetLeaderID() uint64                   { return 0 }
func (p *guardTestProposer) GetMetadata() []byte                   { return nil }
func (p *guardTestProposer) HandleMessage(uint64, *protos.Message) {}

type guardTestProposerBuilder struct {
	starts atomic.Int64
}

func (b *guardTestProposerBuilder) NewProposer(uint64, uint64, uint64, uint64, int) (Proposer, Phase) {
	b.starts.Add(1)
	return &guardTestProposer{}, COMMITTED
}

func TestAbortViewWaitsUntilTheViewStops(t *testing.T) {
	proposer := &guardTestProposer{
		abortStarted: make(chan struct{}),
		abortRelease: make(chan struct{}),
	}

	// No controller run loop is started: a blocking request handler would never answer.
	controller := &Controller{
		currView:       proposer,
		currViewNumber: 1,
		Batcher:        guardTestBatcher{},
		Logger:         newQuietLogger(),
	}

	aborted := make(chan bool, 1)
	go func() {
		aborted <- controller.AbortView(1)
	}()

	select {
	case <-proposer.abortStarted:
	case <-time.After(time.Second):
		require.Fail(t, "the view was not aborted")
	}

	select {
	case <-aborted:
		require.Fail(t, "AbortView returned before the view stopped")
	case <-time.After(100 * time.Millisecond):
	}

	close(proposer.abortRelease)

	select {
	case ok := <-aborted:
		require.True(t, ok)
	case <-time.After(time.Second):
		require.Fail(t, "AbortView did not return after the view stopped")
	}

	require.True(t, proposer.Stopped())
}

func TestAbortViewAndHoldBlocksViewChangesUntilReleased(t *testing.T) {
	builder := &guardTestProposerBuilder{}

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &Controller{
		Checkpoint:      &types.Checkpoint{},
		Batcher:         guardTestBatcher{},
		RequestPool:     guardTestRequestPool{},
		LeaderMonitor:   guardTestLeaderMonitor{},
		ProposerBuilder: builder,
		ID:              0,
		N:               4,
		NodesList:       []uint64{0, 1, 2, 3},
		Logger:          newQuietLogger(),
		StartedWG:       &startedWG,
	}

	controller.Start(1, 0, 0, 0, false)
	defer controller.Stop()

	require.True(t, controller.AbortViewAndHold(1))

	changed := make(chan struct{})
	go func() {
		controller.changeView(1, 1, 0)
		close(changed)
	}()

	select {
	case <-changed:
		t.Fatal("changeView returned while the view changer was writing its record")
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, int64(1), builder.starts.Load(), "the view was restarted before the view changer released it")

	controller.ReleaseViewWrite()

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("changeView did not continue after the view changer released the write")
	}

	require.Equal(t, int64(2), builder.starts.Load())
}

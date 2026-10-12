// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"testing"

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
	aborted atomic.Bool
}

func (p *guardTestProposer) Propose(types.Proposal)                {}
func (p *guardTestProposer) Start()                                {}
func (p *guardTestProposer) Abort()                                { p.aborted.Store(true) }
func (p *guardTestProposer) AbortWithCancel(<-chan struct{})       { p.Abort() }
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

func TestChangeViewDoesNotRestartViewLeftByViewChanger(t *testing.T) {
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

	require.Equal(t, int64(1), builder.starts.Load())

	controller.holdView(1)
	require.True(t, controller.isViewHeld(1))

	controller.changeView(1, 1, 0)

	require.Equal(t, int64(1), builder.starts.Load(), "the controller restarted the view the view changer left")

	controller.releaseHeldView()
	require.False(t, controller.isViewHeld(1))

	controller.changeView(1, 1, 0)

	require.Equal(t, int64(2), builder.starts.Load(), "the view was not restarted after the hold was released")
}

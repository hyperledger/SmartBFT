// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	heartbeatTimeout  = 60 * time.Second
	heartbeatCount    = 10
	tickIncrementUnit = heartbeatTimeout / heartbeatCount
)

var (
	heartbeat                   = makeHeartBeat(10, 10)
	heartbeatFromFarAheadLeader = makeHeartBeat(10, 15)
)

func TestHeartbeatMonitor_New(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	comm := &mocks.CommMock{}
	handler := &mocks.HeartbeatEventHandler{}

	scheduler := make(chan time.Time)
	hm := bft.NewHeartbeatMonitor(scheduler, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm, 4, handler, &atomic.Value{}, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)
	require.NotNil(t, hm)
	hm.Close()
}

func TestHeartbeatMonitorLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	comm := &mocks.CommMock{}
	handler := &mocks.HeartbeatEventHandler{}
	scheduler := make(chan time.Time)

	vs := &atomic.Value{}
	vs.Store(bft.ViewSequence{ViewActive: true})
	hm := bft.NewHeartbeatMonitor(scheduler, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm, 4, handler, vs, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)

	var heartBeatsSent uint32
	var heartBeatsSentUntilViewBecomesInactive uint32

	var toWG1 sync.WaitGroup
	toWG1.Add(10)
	var toWG2 sync.WaitGroup
	toWG2.Add(10)
	comm.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		view := msg.GetHeartBeat().GetView()
		atomic.AddUint32(&heartBeatsSent, 1)
		if uint64(10) == view {
			toWG1.Done()
		} else if uint64(20) == view {
			toWG2.Done()
			totalHBsSent := atomic.LoadUint32(&heartBeatsSent)
			if totalHBsSent == 20 {
				// View is stopped
				vs.Store(bft.ViewSequence{ViewActive: false, ProposalSeq: msg.GetHeartBeat().GetSeq()})
				// Record HB number we sent so far
				atomic.StoreUint32(&heartBeatsSentUntilViewBecomesInactive, totalHBsSent)
			}
		}
	}).Return()

	clock := fakeTime{}
	hm.ChangeRole(bft.Leader, 10, 12)
	clock.advanceTime(11, scheduler)
	toWG1.Wait()

	hm.ChangeRole(bft.Leader, 20, 12)
	clock.advanceTime(10, scheduler)
	toWG2.Wait()

	clock.advanceTime(10, scheduler)
	// Ensure we don't advance heartbeats any longer when view is inactive
	require.Equal(t, atomic.LoadUint32(&heartBeatsSentUntilViewBecomesInactive), atomic.LoadUint32(&heartBeatsSent))

	hm.Close()
}

func TestHeartbeatMonitorFollower(t *testing.T) {
	noop := func(_ *bft.HeartbeatMonitor) {}

	for _, testCase := range []struct {
		description                 string
		onHeartbeatTimeoutCallCount int
		heartbeatMessage            *smartbftprotos.Message
		event                       func(*bft.HeartbeatMonitor)
		sender                      uint64
		viewActive                  bool
		proposalSeqInView           uint64
		syncCalled                  bool
	}{
		{
			description:                 "timeout expires",
			sender:                      12,
			heartbeatMessage:            &smartbftprotos.Message{},
			onHeartbeatTimeoutCallCount: 1,
			event:                       noop,
			proposalSeqInView:           10,
		},
		{
			description:       "heartbeats prevent timeout",
			sender:            12,
			heartbeatMessage:  heartbeat,
			event:             noop,
			proposalSeqInView: 10,
		},
		{
			description:       "heartbeats from leader with inactive view don't prevent timeout",
			sender:            12,
			heartbeatMessage:  heartbeat,
			event:             noop,
			proposalSeqInView: 10,
		},
		{
			description:                 "bad heartbeats do not prevent timeout",
			sender:                      12,
			heartbeatMessage:            prePrepare,
			onHeartbeatTimeoutCallCount: 1,
			event:                       noop,
			proposalSeqInView:           10,
		},
		{
			description:                 "heartbeats not from the leader do not prevent timeout",
			sender:                      13,
			heartbeatMessage:            heartbeat,
			onHeartbeatTimeoutCallCount: 1,
			event:                       noop,
			proposalSeqInView:           10,
		},
		{
			description:                 "heartbeats from a leader too far ahead lead to sync and timeout",
			sender:                      12,
			heartbeatMessage:            heartbeatFromFarAheadLeader,
			onHeartbeatTimeoutCallCount: 1,
			event:                       noop,
			proposalSeqInView:           10,
			viewActive:                  true,
			syncCalled:                  true,
		},
		{
			description:       "heartbeats from a leader only 1 seq ahead do not lead to timeout",
			sender:            12,
			heartbeatMessage:  heartbeatFromFarAheadLeader,
			event:             noop,
			proposalSeqInView: 14,
			viewActive:        true,
		},
		{
			description:                 "heartbeats from a leader too far ahead when view is disabled do not cause timeouts",
			sender:                      12,
			heartbeatMessage:            heartbeatFromFarAheadLeader,
			onHeartbeatTimeoutCallCount: 0,
			event:                       noop,
			proposalSeqInView:           10,
		},
		{
			description:                 "view change to dead leader",
			sender:                      12,
			onHeartbeatTimeoutCallCount: 1,
			event: func(hm *bft.HeartbeatMonitor) {
				hm.ChangeRole(bft.Follower, 11, 12)
			},
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			require.NoError(t, err)
			log := basicLog.Sugar()

			scheduler := make(chan time.Time)
			incrementUnit := heartbeatTimeout / heartbeatCount

			comm := &mocks.CommMock{}
			handler := &mocks.HeartbeatEventHandler{}
			handler.On("OnHeartbeatTimeout", uint64(10), uint64(12))
			handler.On("OnHeartbeatTimeout", uint64(11), uint64(12))
			handler.On("Sync")

			viewSequence := &atomic.Value{}
			viewSequence.Store(bft.ViewSequence{
				ViewActive:  testCase.viewActive,
				ProposalSeq: testCase.proposalSeqInView,
			})
			hm := bft.NewHeartbeatMonitor(scheduler, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm, 4, handler, viewSequence, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)

			hm.ChangeRole(bft.Follower, 10, 12)

			start := time.Now()
			scheduler <- start
			hm.ProcessMsg(12, heartbeat)
			testCase.event(hm)

			start = start.Add(incrementUnit).Add(time.Second)

			for i := 1; i <= heartbeatCount*2; i++ {
				elapsed := start.Add(time.Duration(i)*incrementUnit + time.Millisecond)
				scheduler <- elapsed
				hm.ProcessMsg(testCase.sender, testCase.heartbeatMessage)
			}
			hm.Close()

			handler.AssertNumberOfCalls(t, "OnHeartbeatTimeout", testCase.onHeartbeatTimeoutCallCount)
			if testCase.syncCalled {
				handler.AssertCalled(t, "Sync")
			}
		})
	}
}

func TestHeartbeatMonitorLeaderAndFollower(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	scheduler1 := make(chan time.Time)
	scheduler2 := make(chan time.Time)

	comm1 := &mocks.CommMock{}
	handler1 := &mocks.HeartbeatEventHandler{}
	vs1 := &atomic.Value{}
	vs1.Store(bft.ViewSequence{ViewActive: true})
	hm1 := bft.NewHeartbeatMonitor(scheduler1, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm1, 4, handler1, vs1, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)

	comm2 := &mocks.CommMock{}
	handler2 := &mocks.HeartbeatEventHandler{}
	vs2 := &atomic.Value{}
	vs2.Store(bft.ViewSequence{ViewActive: true})
	hm2 := bft.NewHeartbeatMonitor(scheduler2, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm2, 4, handler2, vs2, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)

	comm1.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		hm2.ProcessMsg(1, msg)
	})

	comm2.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		hm1.ProcessMsg(2, msg)
	})

	toWG := &sync.WaitGroup{}
	toWG.Add(1)
	handler1.On("OnHeartbeatTimeout", uint64(12), uint64(2)).Run(func(args mock.Arguments) {
		view := args[0].(uint64)
		if view != 12 {
			t.Fail()
		} else {
			toWG.Done()
		}
	}).Return()

	clock := fakeTime{}

	hm1.ChangeRole(bft.Leader, 10, 1)
	hm2.ChangeRole(bft.Follower, 10, 1)
	clock.advanceTime(heartbeatCount*2, scheduler1, scheduler2)

	hm1.ChangeRole(bft.Follower, 11, 2)
	hm2.ChangeRole(bft.Leader, 11, 2)
	clock.advanceTime(heartbeatCount*2, scheduler1, scheduler2)

	// first advance the leader to avoid sending hb-response
	hm2.ChangeRole(bft.Leader, 12, 2)
	hm1.ChangeRole(bft.Follower, 12, 2)

	hm2.Close()
	clock.advanceTime(heartbeatCount*2, scheduler1)
	toWG.Wait()
	hm1.Close()

	handler1.AssertCalled(t, "OnHeartbeatTimeout", uint64(12), uint64(2))
	handler1.AssertNumberOfCalls(t, "OnHeartbeatTimeout", 1)
}

func TestHeartbeatResponseLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	clock := &fakeTime{time: time.Now()}

	scheduler1 := make(chan time.Time)
	comm1 := &mocks.CommMock{}
	handler1 := &mocks.HeartbeatEventHandler{}
	vs1 := &atomic.Value{}
	vs1.Store(bft.ViewSequence{ViewActive: true, ProposalSeq: 12})
	hm1 := bft.NewHeartbeatMonitor(scheduler1, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm1, 7, handler1, vs1, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)

	comm1.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		hb := msg.GetHeartBeat()
		require.NotNil(t, hb)
		require.Equal(t, uint64(5), hb.GetView())
		require.Equal(t, uint64(12), hb.GetSeq())
	})

	hm1.ChangeRole(bft.Leader, 5, 0)
	clock.advanceTime(2*heartbeatCount, scheduler1)

	// this will be ignored, as view=4,5 <= leader-view=5
	hbr4 := makeHeartBeatResponse(4)
	hm1.ProcessMsg(1, hbr4)
	hm1.ProcessMsg(2, hbr4)
	hm1.ProcessMsg(3, hbr4)
	hbr5 := makeHeartBeatResponse(5)
	hm1.ProcessMsg(1, hbr5)
	hm1.ProcessMsg(2, hbr5)
	hm1.ProcessMsg(3, hbr5)

	syncWG := &sync.WaitGroup{}
	syncWG.Add(1)
	handler1.On("Sync").Run(func(args mock.Arguments) {
		syncWG.Done()
	}).Return()

	// 2 is not enough, need f+1=3, duplicates are filtered
	hbr6 := makeHeartBeatResponse(6)
	hm1.ProcessMsg(1, hbr6)
	hm1.ProcessMsg(2, hbr6)
	hm1.ProcessMsg(1, hbr6)
	hm1.ProcessMsg(2, hbr6)

	// trigger sync
	hbr12 := makeHeartBeatResponse(12)
	hm1.ProcessMsg(1, hbr12)
	hm1.ProcessMsg(2, hbr12)
	hm1.ProcessMsg(3, hbr12)
	// only trigger once before role change
	hbr13 := makeHeartBeatResponse(13)
	hm1.ProcessMsg(1, hbr13)
	hm1.ProcessMsg(2, hbr13)
	hm1.ProcessMsg(3, hbr13)

	syncWG.Wait()
	hm1.Close()

	handler1.AssertCalled(t, "Sync")
	handler1.AssertNumberOfCalls(t, "Sync", 1)
}

func TestHeartbeatResponseFollower(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	clock := &fakeTime{time: time.Now()}

	scheduler1 := make(chan time.Time)
	comm1 := &mocks.CommMock{}
	handler1 := &mocks.HeartbeatEventHandler{}
	syncWG := &sync.WaitGroup{}
	syncWG.Add(1)
	handler1.On("Sync").Run(func(args mock.Arguments) {
		syncWG.Done()
	})
	vs1 := &atomic.Value{}
	vs1.Store(bft.ViewSequence{ViewActive: true, ProposalSeq: 12})
	hm1 := bft.NewHeartbeatMonitor(scheduler1, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm1, 7, handler1, vs1, types.DefaultConfig.NumOfTicksBehindBeforeSyncing)

	respWG := &sync.WaitGroup{}
	respWG.Add(1)
	comm1.On("SendConsensus", mock.AnythingOfType("uint64"), mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		target := args[0].(uint64)
		require.Equal(t, uint64(2), target)
		msg := args[1].(*smartbftprotos.Message)
		hbr := msg.GetHeartBeatResponse()
		require.NotNil(t, hbr)
		require.Equal(t, uint64(6), hbr.GetView())
		respWG.Done()
	})

	hb5 := makeHeartBeat(5, 12)
	hb6 := makeHeartBeat(6, 12)
	hb7 := makeHeartBeat(7, 12)

	hm1.ChangeRole(bft.Follower, 5, 1)
	clock.advanceTime(1, scheduler1)

	hm1.ProcessMsg(1, hb5)

	hm1.ChangeRole(bft.Follower, 6, 2)
	clock.advanceTime(1, scheduler1)

	hm1.ProcessMsg(2, hb6)

	// trigger response
	hm1.ProcessMsg(2, hb5)
	respWG.Wait()

	// trigger sync
	hm1.ProcessMsg(2, hb7)
	syncWG.Wait()

	hm1.Close()
}

func TestFollowerBehindSync(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	scheduler := make(chan time.Time)

	comm := &mocks.CommMock{}
	handler := &mocks.HeartbeatEventHandler{}
	syncWG := &sync.WaitGroup{}
	syncWG.Add(1)
	handler.On("Sync").Run(func(args mock.Arguments) {
		syncWG.Done()
	})

	vs := &atomic.Value{}
	vs.Store(bft.ViewSequence{ViewActive: true, ProposalSeq: 9})
	hm := bft.NewHeartbeatMonitor(scheduler, log, types.DefaultConfig.LeaderHeartbeatTimeout, types.DefaultConfig.LeaderHeartbeatCount, comm, 4, handler, vs, 3)

	hm.ChangeRole(bft.Follower, 10, 12)

	hm.ProcessMsg(12, heartbeat)
	hm.ProcessMsg(12, heartbeat)

	start := time.Now()
	scheduler <- start

	scheduler <- start.Add(time.Second)
	handler.AssertNotCalled(t, "Sync")

	scheduler <- start.Add(2 * time.Second)
	syncWG.Wait()
	handler.AssertCalled(t, "Sync")

	scheduler <- start.Add(3 * time.Second)
	scheduler <- start.Add(4 * time.Second)

	hm.Close()
	handler.AssertNumberOfCalls(t, "Sync", 1)
}

type fakeTime struct {
	time time.Time
}

// each tick is (heartbeatTimeout / heartbeatCount)
func (t *fakeTime) advanceTime(ticks int, schedulers ...chan time.Time) {
	for i := 1; i <= ticks; i++ {
		newTime := t.time.Add(tickIncrementUnit)
		for _, scheduler := range schedulers {
			scheduler <- newTime
		}
		t.time = newTime
	}
}

func makeHeartBeatResponse(view uint64) *smartbftprotos.Message {
	hbr := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeatResponse{
			HeartBeatResponse: &smartbftprotos.HeartBeatResponse{
				View: view,
			},
		},
	}
	return hbr
}

func makeHeartBeat(view, seq uint64) *smartbftprotos.Message {
	hb := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: view,
				Seq:  seq,
			},
		},
	}
	return hb
}

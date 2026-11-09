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

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	heartbeatTimeout  = 60 * time.Second
	heartbeatCount    = 10
	tickIncrementUnit = heartbeatTimeout / heartbeatCount
)

type fakeTime struct {
	time time.Time
}

type commMock struct {
	heartBeatsSent *uint32
	toWG           *sync.WaitGroup
}

func (c commMock) SendConsensus(targetID uint64, m *smartbftprotos.Message) {
	panic("implement me")
}

func (c commMock) SendTransaction(targetID uint64, request []byte) {
	panic("implement me")
}

func (c commMock) Nodes() []uint64 {
	panic("implement me")
}

func (c commMock) BroadcastConsensus(m *smartbftprotos.Message) {
	atomic.AddUint32(c.heartBeatsSent, 1)
	c.toWG.Done()
}

type heartbeatEventHandler struct{}

func (h heartbeatEventHandler) OnHeartbeatTimeout(view uint64, leaderID uint64) {
	panic("implement me")
}

func (h heartbeatEventHandler) Sync() {
	panic("implement me")
}

func TestHeartbeatWasSent(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	var heartBeatsSent uint32
	var toWG sync.WaitGroup

	comm := &commMock{heartBeatsSent: &heartBeatsSent, toWG: &toWG}
	handler := &heartbeatEventHandler{}
	scheduler := make(chan time.Time)

	vs := &atomic.Value{}
	vs.Store(ViewSequence{ViewActive: true})
	hm := NewHeartbeatMonitor(scheduler, log, heartbeatTimeout, heartbeatCount, comm, 4, handler, vs, 10)

	toWG.Add(2)

	clock := fakeTime{}
	hm.ChangeRole(Leader, 10, 12)
	scheduler <- clock.time.Add(tickIncrementUnit)
	scheduler <- clock.time.Add(tickIncrementUnit * 2)                     // sending heartbeat
	scheduler <- clock.time.Add(tickIncrementUnit*2 + tickIncrementUnit/2) // not sending yet

	hm.HeartbeatWasSent()
	hm.sentHeartbeat <- struct{}{} // make sure that the heartbeat monitor read this channel

	scheduler <- clock.time.Add(tickIncrementUnit * 3) // not sending because sent already (by HeartbeatWasSent)
	scheduler <- clock.time.Add(tickIncrementUnit * 4) // sending now
	toWG.Wait()

	hm.Close()

	require.Equal(t, uint32(2), atomic.LoadUint32(&heartBeatsSent))
}

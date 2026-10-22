// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var noopTimeoutHandler = &mocks.RequestTimeoutHandler{}

func init() {
	noopTimeoutHandler.On("OnRequestTimeout", mock.Anything, mock.Anything)
	noopTimeoutHandler.On("OnLeaderFwdRequestTimeout", mock.Anything, mock.Anything)
}

func TestBatcherBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	submittedChan := make(chan struct{}, 1)

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo-bar")
	byteReq3 := makeTestRequest("3", "3", "foo-bar-foo")
	byteReq4 := makeTestRequest("4", "4", "foo-bar-foo-bar")
	byteReq5 := makeTestRequest("5", "5", "foo-bar-foo-bar-foo")
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3}, submittedChan)
	err = pool.Submit(byteReq1) // pool: [req1]
	require.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 1, 2048, 10*time.Millisecond)

	res := batcher.NextBatch()
	require.Len(t, res, 1)

	res = batcher.NextBatch()
	require.Len(t, res, 1)

	err = pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"}) // pool: []
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Empty(t, res) // after timeout

	err = pool.Submit(byteReq2)
	require.NoError(t, err)
	err = pool.Submit(byteReq3) // pool: [req3, req2]
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq2, res[0])

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq2, res[0])

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq2, res[0])

	err = pool.RemoveRequest(types.RequestInfo{ID: "2", ClientID: "2"}) // pool: [req3]
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq3, res[0])

	// count limit
	batcher = bft.NewBatchBuilder(pool, submittedChan, 2, 2048, 10*time.Millisecond)

	err = pool.Submit(byteReq4) // pool: [req4, req3]
	require.NoError(t, err)
	err = pool.Submit(byteReq5) // pool: [req5, req4, req3]
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Len(t, res, 2)
	require.Equal(t, byteReq3, res[0])
	require.Equal(t, byteReq4, res[1])

	// size limit
	batcher = bft.NewBatchBuilder(pool, submittedChan, 10, uint64(len(byteReq3)+len(byteReq4)), 10*time.Millisecond)

	res = batcher.NextBatch()
	require.Len(t, res, 2)
	require.Equal(t, byteReq3, res[0])
	require.Equal(t, byteReq4, res[1])

	// high limits
	batcher = bft.NewBatchBuilder(pool, submittedChan, 10, 2048, 10*time.Millisecond)
	res = batcher.NextBatch()
	require.Len(t, res, 3)
	require.Equal(t, byteReq3, res[0])
	require.Equal(t, byteReq4, res[1])
	require.Equal(t, byteReq5, res[2])

	pool.Close()
}

func TestBatcherWhileSubmitting(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	submittedChan := make(chan struct{}, 1)
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 200}, submittedChan)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 100, 5000, 100*time.Second)

	submitDone := make(chan error, 1)
	go func() {
		for i := range 300 {
			iStr := strconv.Itoa(i)
			byteReq := makeTestRequest(iStr, iStr, "foo")
			if err := pool.Submit(byteReq); err != nil {
				submitDone <- err
				return
			}
		}
		submitDone <- nil
	}()

	res := batcher.NextBatch()
	require.Len(t, res, 100)
	for i := range 100 {
		iStr := fmt.Sprintf("%d", i)
		reqInfo := insp.RequestID(res[i])
		require.Equal(t, iStr, reqInfo.ID)
		err := pool.RemoveRequest(reqInfo)
		require.NoError(t, err)
	}

	res = batcher.NextBatch()
	require.Len(t, res, 100)
	for i := range 100 {
		iStr := fmt.Sprintf("%d", i+100)
		reqInfo := insp.RequestID(res[i])
		require.Equal(t, iStr, reqInfo.ID)
		err := pool.RemoveRequest(reqInfo)
		require.NoError(t, err)
	}

	res = batcher.NextBatch()
	require.Len(t, res, 100)
	for i := range 100 {
		iStr := fmt.Sprintf("%d", i+200)
		reqInfo := insp.RequestID(res[i])
		require.Equal(t, iStr, reqInfo.ID)
		err := pool.RemoveRequest(reqInfo)
		require.NoError(t, err)
	}

	pool.Close()

	require.NoError(t, <-submitDone)
}

func TestBatcherClose(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	submittedChan := make(chan struct{}, 1)
	byteReq := makeTestRequest("1", "1", "foo")
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3}, submittedChan)
	err = pool.Submit(byteReq)
	require.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 100, 2048, time.Minute)

	go func() {
		batcher.Close()
	}()

	t1 := time.Now()
	res := batcher.NextBatch()
	require.Nil(t, res)
	require.Less(t, time.Since(t1), time.Second*50)
	pool.Close()
}

func TestBatcherReset(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	submittedChan := make(chan struct{}, 1)
	byteReq1 := makeTestRequest("1", "1", "foo")
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3}, submittedChan)
	err = pool.Submit(byteReq1)
	require.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 1, 2048, 10*time.Millisecond)

	res := batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq1, res[0])

	byteReq2 := makeTestRequest("2", "2", "foo")
	err = pool.Submit(byteReq2)
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq1, res[0])

	batcher.Close()
	batcher.Reset()

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq1, res[0])

	err = pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"})
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Len(t, res, 1)
	require.Equal(t, byteReq2, res[0])

	batcher.Close()
	batcher.Reset()

	err = pool.RemoveRequest(types.RequestInfo{ID: "2", ClientID: "2"})
	require.NoError(t, err)

	res = batcher.NextBatch()
	require.Empty(t, res)
	pool.Close()
}

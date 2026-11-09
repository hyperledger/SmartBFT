// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestBasicTimeout(t *testing.T) {
	t.Parallel()

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	collector := &bft.StateCollector{
		SelfID:         0,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}

	collector.Start()

	collector.ClearCollected()
	response := collector.CollectStateResponses()
	require.Nil(t, response)

	collector.Stop()
}

func TestCollect(t *testing.T) {
	msgV1S1 := &protos.Message{
		Content: &protos.Message_StateTransferResponse{
			StateTransferResponse: &protos.StateTransferResponse{
				ViewNum:  1,
				Sequence: 1,
			},
		},
	}
	msgV1S2 := &protos.Message{
		Content: &protos.Message_StateTransferResponse{
			StateTransferResponse: &protos.StateTransferResponse{
				ViewNum:  1,
				Sequence: 2,
			},
		},
	}
	msgV1S3 := &protos.Message{
		Content: &protos.Message_StateTransferResponse{
			StateTransferResponse: &protos.StateTransferResponse{
				ViewNum:  1,
				Sequence: 3,
			},
		},
	}
	for _, test := range []struct {
		description string
		timeout     time.Duration
		msg1        *protos.Message
		msg2        *protos.Message
		msg3        *protos.Message
		isNil       bool
		view        uint64
		seq         uint64
	}{
		{
			description: "all same responses",
			timeout:     10 * time.Minute,
			msg1:        msgV1S1,
			msg2:        msgV1S1,
			msg3:        msgV1S1,
			isNil:       false,
			view:        1,
			seq:         1,
		},
		{
			description: "enough responses",
			timeout:     10 * time.Minute,
			msg1:        msgV1S3,
			msg2:        msgV1S3,
			msg3:        nil,
			isNil:       false,
			view:        1,
			seq:         3,
		},
		{
			description: "all different responses",
			timeout:     100 * time.Millisecond,
			msg1:        msgV1S1,
			msg2:        msgV1S2,
			msg3:        msgV1S3,
			isNil:       true,
		},
		{
			description: "not enough responses",
			timeout:     100 * time.Millisecond,
			msg1:        msgV1S1,
			msg2:        nil,
			msg3:        nil,
			isNil:       true,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			require.NoError(t, err)
			startWG := sync.WaitGroup{}
			startWG.Add(1)
			log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, "started collecting") {
					startWG.Done()
				}
				return nil
			})).Sugar()

			collector := &bft.StateCollector{
				SelfID:         0,
				N:              4,
				Logger:         log,
				CollectTimeout: test.timeout,
			}

			collector.Start()
			collector.ClearCollected()
			responseChan := make(chan *types.ViewAndSeq)

			go func() {
				responseChan <- collector.CollectStateResponses()
			}()

			startWG.Wait()

			if test.msg1 != nil {
				collector.HandleMessage(1, test.msg1)
			}
			if test.msg2 != nil {
				collector.HandleMessage(2, test.msg2)
			}
			if test.msg3 != nil {
				collector.HandleMessage(3, test.msg3)
			}

			response := <-responseChan

			if test.isNil {
				require.Nil(t, response)
			} else {
				require.NotNil(t, response)
				require.Equal(t, test.view, response.View)
				require.Equal(t, test.seq, response.Seq)
			}
			collector.Stop()
		})
	}
}

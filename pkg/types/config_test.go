// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types_test

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestConfigurationValidate(t *testing.T) {
	validConfig := func() types.Configuration {
		config := types.DefaultConfig
		config.SelfID = 1
		return config
	}

	assert.NoError(t, validConfig().Validate())

	for _, tst := range []struct {
		name        string
		mutate      func(*types.Configuration)
		expectedErr string
	}{
		{
			name:        "zero SelfID",
			mutate:      func(c *types.Configuration) { c.SelfID = 0 },
			expectedErr: "SelfID should be greater than zero",
		},
		{
			name:        "zero RequestBatchMaxCount",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxCount = 0 },
			expectedErr: "RequestBatchMaxCount should be greater than zero",
		},
		{
			name:        "zero RequestBatchMaxBytes",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxBytes = 0 },
			expectedErr: "RequestBatchMaxBytes should be greater than zero",
		},
		{
			name:        "zero RequestBatchMaxInterval",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxInterval = 0 },
			expectedErr: "RequestBatchMaxInterval should be greater than zero",
		},
		{
			name:        "negative RequestBatchMaxInterval",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxInterval = -time.Second },
			expectedErr: "RequestBatchMaxInterval should be greater than zero",
		},
		{
			name:        "zero IncomingMessageBufferSize",
			mutate:      func(c *types.Configuration) { c.IncomingMessageBufferSize = 0 },
			expectedErr: "IncomingMessageBufferSize should be greater than zero",
		},
		{
			name:        "zero RequestPoolSize",
			mutate:      func(c *types.Configuration) { c.RequestPoolSize = 0 },
			expectedErr: "RequestPoolSize should be greater than zero",
		},
		{
			name:        "zero RequestForwardTimeout",
			mutate:      func(c *types.Configuration) { c.RequestForwardTimeout = 0 },
			expectedErr: "RequestForwardTimeout should be greater than zero",
		},
		{
			name:        "zero RequestComplainTimeout",
			mutate:      func(c *types.Configuration) { c.RequestComplainTimeout = 0 },
			expectedErr: "RequestComplainTimeout should be greater than zero",
		},
		{
			name:        "zero RequestAutoRemoveTimeout",
			mutate:      func(c *types.Configuration) { c.RequestAutoRemoveTimeout = 0 },
			expectedErr: "RequestAutoRemoveTimeout should be greater than zero",
		},
		{
			name:        "zero ViewChangeResendInterval",
			mutate:      func(c *types.Configuration) { c.ViewChangeResendInterval = 0 },
			expectedErr: "ViewChangeResendInterval should be greater than zero",
		},
		{
			name:        "zero ViewChangeTimeout",
			mutate:      func(c *types.Configuration) { c.ViewChangeTimeout = 0 },
			expectedErr: "ViewChangeTimeout should be greater than zero",
		},
		{
			name:        "zero LeaderHeartbeatTimeout",
			mutate:      func(c *types.Configuration) { c.LeaderHeartbeatTimeout = 0 },
			expectedErr: "LeaderHeartbeatTimeout should be greater than zero",
		},
		{
			name:        "zero LeaderHeartbeatCount",
			mutate:      func(c *types.Configuration) { c.LeaderHeartbeatCount = 0 },
			expectedErr: "LeaderHeartbeatCount should be greater than zero",
		},
		{
			name:        "zero NumOfTicksBehindBeforeSyncing",
			mutate:      func(c *types.Configuration) { c.NumOfTicksBehindBeforeSyncing = 0 },
			expectedErr: "NumOfTicksBehindBeforeSyncing should be greater than zero",
		},
		{
			name:        "zero CollectTimeout",
			mutate:      func(c *types.Configuration) { c.CollectTimeout = 0 },
			expectedErr: "CollectTimeout should be greater than zero",
		},
		{
			name: "RequestBatchMaxCount bigger than RequestBatchMaxBytes",
			mutate: func(c *types.Configuration) {
				c.RequestBatchMaxBytes = c.RequestMaxBytes
				c.RequestBatchMaxCount = c.RequestBatchMaxBytes + 1
			},
			expectedErr: "RequestBatchMaxCount is bigger than RequestBatchMaxBytes",
		},
		{
			name:        "RequestMaxBytes bigger than RequestBatchMaxBytes",
			mutate:      func(c *types.Configuration) { c.RequestMaxBytes = c.RequestBatchMaxBytes + 1 },
			expectedErr: "RequestMaxBytes is bigger than RequestBatchMaxBytes",
		},
		{
			name:        "RequestForwardTimeout bigger than RequestComplainTimeout",
			mutate:      func(c *types.Configuration) { c.RequestForwardTimeout = c.RequestComplainTimeout + time.Second },
			expectedErr: "RequestForwardTimeout is bigger than RequestComplainTimeout",
		},
		{
			name:        "RequestComplainTimeout bigger than RequestAutoRemoveTimeout",
			mutate:      func(c *types.Configuration) { c.RequestComplainTimeout = c.RequestAutoRemoveTimeout + time.Second },
			expectedErr: "RequestComplainTimeout is bigger than RequestAutoRemoveTimeout",
		},
		{
			name:        "ViewChangeResendInterval bigger than ViewChangeTimeout",
			mutate:      func(c *types.Configuration) { c.ViewChangeResendInterval = c.ViewChangeTimeout + time.Second },
			expectedErr: "ViewChangeResendInterval is bigger than ViewChangeTimeout",
		},
		{
			name: "leader rotation without DecisionsPerLeader",
			mutate: func(c *types.Configuration) {
				c.LeaderRotation = true
				c.DecisionsPerLeader = 0
			},
			expectedErr: "DecisionsPerLeader should be greater than zero when leader rotation is active",
		},
		{
			name: "DecisionsPerLeader without leader rotation",
			mutate: func(c *types.Configuration) {
				c.LeaderRotation = false
				c.DecisionsPerLeader = 1
			},
			expectedErr: "DecisionsPerLeader should be zero when leader rotation is off",
		},
		{
			name:        "zero RequestMaxBytes",
			mutate:      func(c *types.Configuration) { c.RequestMaxBytes = 0 },
			expectedErr: "RequestMaxBytes should be greater than zero",
		},
		{
			name:        "zero RequestPoolSubmitTimeout",
			mutate:      func(c *types.Configuration) { c.RequestPoolSubmitTimeout = 0 },
			expectedErr: "RequestPoolSubmitTimeout should be greater than zero",
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			config := validConfig()
			tst.mutate(&config)
			assert.EqualError(t, config.Validate(), tst.expectedErr)
		})
	}

	t.Run("leader rotation off", func(t *testing.T) {
		config := validConfig()
		config.LeaderRotation = false
		config.DecisionsPerLeader = 0
		assert.NoError(t, config.Validate())
	})
}

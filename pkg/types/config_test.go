// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types

import (
	"testing"
	"time"
)

func TestDefaultPrepareVoteCollectionTimeout(t *testing.T) {
	if DefaultConfig.PrepareVoteCollectionTimeout != 5*time.Minute {
		t.Fatalf("expected five-minute prepare timeout, got %s", DefaultConfig.PrepareVoteCollectionTimeout)
	}
	if DefaultConfig.RequestTimerGuardDuringPrepare {
		t.Fatal("generic SmartBFT request-timer guard must remain opt-in")
	}
}

func TestPrepareVoteCollectionTimeoutValidation(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		config := DefaultConfig
		config.SelfID = 1
		config.PrepareVoteCollectionTimeout = timeout
		if err := config.Validate(); err == nil {
			t.Fatalf("expected timeout %s to be rejected", timeout)
		}
	}
}

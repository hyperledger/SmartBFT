// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"
)

func TestMWVNConsensusConfigAlignsRequestTimersWithPrepareBuffer(t *testing.T) {
	config := mwvnConsensusConfig(2, 5*time.Minute)

	if config.PrepareVoteCollectionTimeout != 5*time.Minute {
		t.Fatalf("unexpected PREPARE timeout: %s", config.PrepareVoteCollectionTimeout)
	}
	if !config.RequestTimerGuardDuringPrepare {
		t.Fatal("MWVN request-timer guard is not enabled")
	}
	if config.RequestComplainTimeout != 5*time.Minute+30*time.Second {
		t.Fatalf("unexpected request complaint timeout: %s", config.RequestComplainTimeout)
	}
	if config.RequestAutoRemoveTimeout != 6*time.Minute {
		t.Fatalf("unexpected request auto-remove timeout: %s", config.RequestAutoRemoveTimeout)
	}
	if config.LeaderHeartbeatTimeout != time.Minute {
		t.Fatalf("heartbeat failure detection must remain independent, got %s", config.LeaderHeartbeatTimeout)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("MWVN consensus configuration is invalid: %v", err)
	}
}

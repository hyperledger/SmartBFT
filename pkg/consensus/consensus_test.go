// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"testing"
	"time"

	algorithm "github.com/hyperledger/SmartBFT/internal/bft"
)

// TestComplainDoesNotBlockDuringReconfiguration covers the shutdown deadlock:
// a reconfiguration holds consensusLock and waits for the views to stop, while
// a view calls Complain and waits for that lock.
func TestComplainDoesNotBlockDuringReconfiguration(t *testing.T) {
	consensus := &Consensus{viewChanger: &algorithm.ViewChanger{}}

	consensus.consensusLock.Lock()
	defer consensus.consensusLock.Unlock()

	done := make(chan struct{})
	go func() {
		consensus.Complain(1, true)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Complain blocked while a reconfiguration held the lock")
	}
}

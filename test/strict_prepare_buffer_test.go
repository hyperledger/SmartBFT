// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type prepareBufferEvents struct {
	suppressed chan time.Time
	timedOut   chan time.Time
	viewChange chan time.Time
}

func configureStrictPrepareTimers(nodes []*App, prepareTimeout time.Duration) {
	for _, node := range nodes {
		node.Consensus.Config.PrepareVoteCollectionTimeout = prepareTimeout
		node.Consensus.Config.RequestTimerGuardDuringPrepare = true
		node.Consensus.Config.RequestForwardTimeout = 100 * time.Millisecond
		node.Consensus.Config.RequestComplainTimeout = 200 * time.Millisecond
		node.Consensus.Config.RequestAutoRemoveTimeout = 300 * time.Millisecond
		// The test scheduler ticks once per second. Three seconds gives healthy
		// heartbeats enough scheduling margin while remaining much shorter than
		// the production five-minute PREPARE buffer.
		node.Consensus.Config.LeaderHeartbeatTimeout = 3 * time.Second
		node.Consensus.Config.LeaderHeartbeatCount = 2
		node.Consensus.Config.ViewChangeResendInterval = 100 * time.Millisecond
		node.Consensus.Config.ViewChangeTimeout = 500 * time.Millisecond
	}
}

func observePrepareBufferEvents(nodes []*App) *prepareBufferEvents {
	events := &prepareBufferEvents{
		suppressed: make(chan time.Time, 16),
		timedOut:   make(chan time.Time, 16),
		viewChange: make(chan time.Time, 16),
	}
	for _, node := range nodes {
		node := node
		logger := node.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			now := time.Now()
			switch {
			case strings.Contains(entry.Message, "buffering PREPARE votes"):
				select {
				case events.suppressed <- now:
				default:
				}
			case strings.Contains(entry.Message, "prepare vote collection timeout"):
				select {
				case events.timedOut <- now:
				default:
				}
			case strings.Contains(entry.Message, "ViewChanged"):
				select {
				case events.viewChange <- now:
				default:
				}
			}
			return nil
		})).Sugar()
		node.logger = logger
		node.Consensus.Logger = logger
	}
	return events
}

func dropPrepareMessages(nodes []*App, drop *atomic.Bool) {
	for _, node := range nodes {
		node.LoseMessages(func(message *smartbftprotos.Message) bool {
			return drop.Load() && message.GetPrepare() != nil
		})
	}
}

func delayPrepareMessagesToNode(nodes []*App, target uint64, release <-chan struct{}) {
	for _, node := range nodes {
		if node.ID == target {
			continue
		}
		node.MutateSend(target, func(_ uint64, message *smartbftprotos.Message) {
			if message.GetPrepare() != nil {
				<-release
			}
		})
	}
}

func awaitEvent(t *testing.T, events <-chan time.Time, timeout time.Duration, description string) time.Time {
	t.Helper()
	select {
	case eventTime := <-events:
		return eventTime
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", description)
		return time.Time{}
	}
}

func assertNoEvent(t *testing.T, events <-chan time.Time, duration time.Duration, description string) {
	t.Helper()
	select {
	case eventTime := <-events:
		t.Fatalf("unexpected %s at %s", description, eventTime.Format(time.RFC3339Nano))
	case <-time.After(duration):
	}
}

func TestStrictPrepareBufferKeepsViewAliveAndReleasesOnEarlyQuorum(t *testing.T) {
	const prepareTimeout = 6 * time.Second
	nodes, network := newFourNodePrepareTimeoutNetwork(t, prepareTimeout)
	configureStrictPrepareTimers(nodes, prepareTimeout)
	for _, node := range nodes {
		node.Consensus.Config.LeaderHeartbeatTimeout = 2 * time.Second
	}
	events := observePrepareBufferEvents(nodes)
	releasePrepares := make(chan struct{})
	delayPrepareMessagesToNode(nodes, nodes[0].ID, releasePrepares)

	startNodes(nodes, network)
	started := time.Now()
	nodes[0].Submit(Request{ID: "approved-qc-strict-early", ClientID: "mwvn-ingress"})
	awaitEvent(t, events.suppressed, 2*time.Second, "active-buffer request-timer suppression")

	// This exceeds the old request-complaint plus heartbeat path that caused the
	// regression, but is still safely before the PREPARE deadline.
	assertNoEvent(t, events.viewChange, 3200*time.Millisecond, "view change before PREPARE deadline")
	close(releasePrepares)
	waitForEqualDeliveries(t, nodes, 10*time.Second)
	if elapsed := time.Since(started); elapsed >= prepareTimeout {
		t.Fatalf("early quorum committed after the PREPARE deadline: %s", elapsed)
	}
	assertNoEvent(t, events.viewChange, 100*time.Millisecond, "view change after early quorum")
}

func TestStrictPrepareBufferExpiresBeforeViewChangeAndRecovers(t *testing.T) {
	const prepareTimeout = 1200 * time.Millisecond
	nodes, network := newFourNodePrepareTimeoutNetwork(t, prepareTimeout)
	configureStrictPrepareTimers(nodes, prepareTimeout)
	events := observePrepareBufferEvents(nodes)
	var drop atomic.Bool
	drop.Store(true)
	dropPrepareMessages(nodes, &drop)

	startNodes(nodes, network)
	started := time.Now()
	nodes[0].Submit(Request{ID: "approved-qc-strict-expiry", ClientID: "mwvn-ingress"})
	awaitEvent(t, events.suppressed, 2*time.Second, "active-buffer request-timer suppression")
	timedOutAt := awaitEvent(t, events.timedOut, 3*time.Second, "PREPARE deadline")
	if elapsed := timedOutAt.Sub(started); elapsed < prepareTimeout-100*time.Millisecond {
		t.Fatalf("PREPARE deadline fired early after %s; configured %s", elapsed, prepareTimeout)
	}
	viewChangedAt := awaitEvent(t, events.viewChange, 3*time.Second, "view change after PREPARE deadline")
	if viewChangedAt.Before(timedOutAt) {
		t.Fatalf("view changed at %s before PREPARE timeout at %s", viewChangedAt, timedOutAt)
	}

	drop.Store(false)
	waitForEqualDeliveries(t, nodes, 10*time.Second)
}

func TestStrictPrepareBufferDoesNotMaskLeaderFailure(t *testing.T) {
	const prepareTimeout = 5 * time.Second
	nodes, network := newFourNodePrepareTimeoutNetwork(t, prepareTimeout)
	configureStrictPrepareTimers(nodes, prepareTimeout)
	for _, node := range nodes {
		node.Consensus.Config.LeaderHeartbeatTimeout = time.Second
	}
	events := observePrepareBufferEvents(nodes)
	var drop atomic.Bool
	drop.Store(true)
	dropPrepareMessages(nodes, &drop)

	startNodes(nodes, network)
	started := time.Now()
	nodes[0].Submit(Request{ID: "approved-qc-strict-leader-failure", ClientID: "mwvn-ingress"})
	awaitEvent(t, events.suppressed, 2*time.Second, "active-buffer request-timer suppression")
	nodes[0].Disconnect()

	viewChangedAt := awaitEvent(t, events.viewChange, 4*time.Second, "heartbeat-driven view change")
	if elapsed := viewChangedAt.Sub(started); elapsed >= prepareTimeout {
		t.Fatalf("leader failure was masked until the PREPARE deadline: %s", elapsed)
	}
}

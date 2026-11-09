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

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type abortTestBatcher struct {
	closed chan struct{}
}

func (abortTestBatcher) NextBatch() [][]byte { return nil }

func (b abortTestBatcher) Close() {
	if b.closed != nil {
		close(b.closed)
	}
}

func (abortTestBatcher) Closed() bool { return false }
func (abortTestBatcher) Reset()       {}

type abortTestLeaderMonitor struct{}

func (abortTestLeaderMonitor) ChangeRole(Role, uint64, uint64)                   {}
func (abortTestLeaderMonitor) ProcessMsg(uint64, *protos.Message)                {}
func (abortTestLeaderMonitor) InjectArtificialHeartbeat(uint64, *protos.Message) {}
func (abortTestLeaderMonitor) HeartbeatWasSent()                                 {}
func (abortTestLeaderMonitor) Close()                                            {}
func (abortTestLeaderMonitor) StopLeaderSendMsg()                                {}

// newAbortTestView returns a view that is not started, but can be aborted. The
// returned end function makes the view finish, as its run loop would.
func newAbortTestView(number uint64) (*View, func()) {
	view := &View{
		Number:    number,
		abortChan: make(chan struct{}),
	}
	view.viewEnded.Add(1)

	var once sync.Once

	return view, func() {
		once.Do(view.viewEnded.Done)
	}
}

// newQuietLogger keeps the test output readable.
func newQuietLogger() api.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}

func TestAbortViewWaitsUntilTheViewStops(t *testing.T) {
	view, endView := newAbortTestView(1)
	defer endView()

	controller := &Controller{
		currView:       view,
		currViewNumber: 1,
		Batcher:        abortTestBatcher{},
		Logger:         newQuietLogger(),
	}

	done := make(chan struct{})
	go func() {
		controller.AbortView(1, nil)
		close(done)
	}()

	select {
	case <-view.AbortChan():
	case <-time.After(time.Second):
		require.Fail(t, "the view was not aborted")
	}

	select {
	case <-done:
		require.Fail(t, "AbortView returned before the view stopped")
	case <-time.After(100 * time.Millisecond):
	}

	endView()

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "AbortView did not return after the view stopped")
	}

	require.True(t, view.Stopped())
}

func TestAbortViewStopsWaitingWhenCancelled(t *testing.T) {
	view, endView := newAbortTestView(1)
	defer endView()

	controller := &Controller{
		currView:       view,
		currViewNumber: 1,
		Batcher:        abortTestBatcher{},
		Logger:         newQuietLogger(),
	}

	cancel := make(chan struct{})
	done := make(chan struct{})
	go func() {
		controller.AbortView(1, cancel)
		close(done)
	}()

	select {
	case <-view.AbortChan():
	case <-time.After(time.Second):
		require.Fail(t, "the view was not aborted")
	}

	close(cancel)

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "AbortView did not return after it was cancelled")
	}

	require.True(t, view.Stopped())
}

// blockingProposerBuilder blocks NewProposer until the test releases it.
type blockingProposerBuilder struct {
	view    Proposer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingProposerBuilder) NewProposer(uint64, uint64, uint64, uint64, int) (Proposer, Phase) {
	b.once.Do(func() {
		close(b.entered)
	})
	<-b.release

	return b.view, COMMITTED
}

// TestAbortViewWaitsForAViewThatIsBeingStarted covers the race between
// changeView and AbortView: changeView has aborted the old view but has not
// started the new one yet, so AbortView must not abort the old view and return
// while changeView goes on to start a running view.
func TestAbortViewWaitsForAViewThatIsBeingStarted(t *testing.T) {
	oldView := &View{
		Number:    1,
		abortChan: make(chan struct{}),
	}

	newView := &View{
		N:             4,
		Number:        2,
		Logger:        newQuietLogger(),
		InMsgQSize:    10,
		MetricsView:   api.NewMetricsView(&disabled.Provider{}),
		ViewSequences: &atomic.Value{},
	}

	builder := &blockingProposerBuilder{
		view:    newView,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	batcher := abortTestBatcher{closed: make(chan struct{})}

	controller := &Controller{
		Checkpoint:      &types.Checkpoint{},
		Batcher:         batcher,
		LeaderMonitor:   abortTestLeaderMonitor{},
		ProposerBuilder: builder,
		ID:              0,
		N:               4,
		NodesList:       []uint64{0, 1, 2, 3},
		Logger:          newQuietLogger(),
		currView:        oldView,
		currViewNumber:  1,
		leaderToken:     make(chan struct{}, 1),
	}

	changed := make(chan struct{})
	go func() {
		controller.changeView(2, 1, 0)
		close(changed)
	}()

	select {
	case <-builder.entered:
	case <-time.After(time.Second):
		require.Fail(t, "changeView did not start a new view")
	}

	aborted := make(chan struct{})
	go func() {
		controller.AbortView(2, nil)
		close(aborted)
	}()

	select {
	case <-batcher.closed:
	case <-time.After(time.Second):
		require.Fail(t, "AbortView did not close the batcher")
	}

	select {
	case <-aborted:
		require.Fail(t, "AbortView returned while changeView was starting a view")
	case <-time.After(100 * time.Millisecond):
	}

	close(builder.release)

	select {
	case <-changed:
	case <-time.After(time.Second):
		require.Fail(t, "changeView did not finish")
	}

	select {
	case <-aborted:
	case <-time.After(time.Second):
		require.Fail(t, "AbortView did not return after changeView finished")
	}

	require.True(t, newView.Stopped(), "AbortView did not abort the view that changeView started")
}

func TestAbortViewAndHoldBlocksViewChangesUntilReleased(t *testing.T) {
	view, endView := newAbortTestView(1)
	defer endView()

	controller := &Controller{
		currView:       view,
		currViewNumber: 1,
		Batcher:        abortTestBatcher{},
		Logger:         newQuietLogger(),
	}

	held := make(chan bool, 1)
	go func() {
		held <- controller.AbortViewAndHold(1, nil)
	}()

	select {
	case <-view.AbortChan():
	case <-time.After(time.Second):
		require.Fail(t, "the view was not aborted")
	}

	endView()

	select {
	case ok := <-held:
		require.True(t, ok)
	case <-time.After(time.Second):
		require.Fail(t, "AbortViewAndHold did not return after the view stopped")
	}

	changed := make(chan struct{})
	go func() {
		controller.changeView(0, 0, 0)
		close(changed)
	}()

	select {
	case <-changed:
		t.Fatal("changeView returned while the view changer was writing its record")
	case <-time.After(50 * time.Millisecond):
	}

	controller.ReleaseViewWrite()

	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("changeView did not continue after the view changer released the write")
	}
}

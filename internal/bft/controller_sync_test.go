// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/pkg/wal"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// stubSynchronizer returns a fixed SyncResponse.
type stubSynchronizer struct {
	response types.SyncResponse
}

func (s *stubSynchronizer) Sync() types.SyncResponse { return s.response }

// stubComm is a no-op implementation of api.Comm.
type stubComm struct{}

func (s *stubComm) SendConsensus(targetID uint64, m *protos.Message) {}
func (s *stubComm) SendTransaction(targetID uint64, request []byte)  {}
func (s *stubComm) Nodes() []uint64                                  { return []uint64{1, 2, 3, 4} }
func (s *stubComm) BroadcastConsensus(m *protos.Message)             {}

// TestSyncDecisionsInView tests that the sync() method returns the correct
// DecisionsInView value in two scenarios:
//   - same height: sync returns the same sequence as the controller, so
//     newDecisionsInView must come from the controller's in-memory state
//     (getCurrentDecisionsInView), not remain zero-initialized.
//   - higher height: sync returns a higher sequence, so newDecisionsInView
//     is overridden from the sync response metadata (+1).
func TestSyncDecisionsInView(t *testing.T) {
	const (
		controllerSeq       = uint64(2365908)
		controllerView      = uint64(782)
		controllerDecisions = uint64(9578)
	)

	// newController creates a minimal Controller with only the fields
	// needed by sync(). The checkpoint metadata sets the controller's
	// current sequence, and the synchronizer controls what sync returns.
	newController := func(t *testing.T, syncResponse types.SyncResponse) *Controller {
		t.Helper()

		basicLog, err := zap.NewDevelopment()
		require.NoError(t, err)
		log := basicLog.Sugar()

		// Checkpoint metadata determines latestSeq() return value.
		checkpoint := &types.Checkpoint{}
		checkpoint.Set(types.Proposal{
			Metadata: MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence:  controllerSeq,
				ViewId:          controllerView,
				DecisionsInView: controllerDecisions,
			}),
		}, nil)

		// StateCollector with very short timeout so fetchState() returns nil
		// without needing real consensus broadcast responses.
		collector := &StateCollector{
			SelfID:         1,
			N:              4,
			Logger:         log,
			CollectTimeout: time.Millisecond,
		}
		collector.Start()
		t.Cleanup(collector.Stop)

		c := &Controller{
			ID:                  1,
			N:                   4,
			Logger:              log,
			Comm:                &stubComm{},
			Synchronizer:        &stubSynchronizer{response: syncResponse},
			Checkpoint:          checkpoint,
			Collector:           collector,
			InFlight:            &InFlightData{},
			ViewChanger:         &ViewChanger{},
			currViewNumber:      controllerView,
			currDecisionsInView: controllerDecisions + 1, // checkpoint stores N, decide() increments to N+1
		}
		// sync() uses grabSyncToken/relinquishSyncToken which need syncChan.
		c.syncChan = make(chan struct{}, 1)

		return c
	}

	t.Run("same_height_must_preserve_decisions", func(t *testing.T) {
		// Synchronizer returns the same sequence as the controller.
		// This simulates the "already at target height" scenario where
		// the orderer is not behind but sync was triggered anyway.
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq,
						ViewId:          controllerView,
						DecisionsInView: controllerDecisions,
					}),
					VerificationSequence: 0,
				},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		})

		viewNum, seq, decisions := c.sync()

		require.Equal(t, controllerView, viewNum, "view number should be preserved")
		require.Equal(t, controllerSeq+1, seq, "proposal sequence should be controllerSeq+1")
		// newDecisionsInView should be initialized from the controller's in-memory
		// state (getCurrentDecisionsInView), which is controllerDecisions+1.
		require.Equal(t, controllerDecisions+1, decisions,
			"DecisionsInView must be preserved when sync returns same height")
	})

	t.Run("higher_height_overrides_decisions", func(t *testing.T) {
		// Synchronizer returns a higher sequence than the controller.
		// In this case newDecisionsInView is overridden from the sync response (+1).
		syncDecisions := controllerDecisions + 1
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq + 1,
						ViewId:          controllerView,
						DecisionsInView: syncDecisions,
					}),
					VerificationSequence: 0,
				},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		})

		viewNum, seq, decisions := c.sync()

		require.Equal(t, controllerView, viewNum, "view number should be preserved")
		require.Equal(t, controllerSeq+2, seq, "proposal sequence should be syncSeq+1")
		require.Equal(t, syncDecisions+1, decisions,
			"DecisionsInView should be latestDecisionDecisions+1 when sync returns higher height")
	})
}

// stubRequestsTimer is a no-op api.RequestsTimer.
type stubRequestsTimer struct{}

func (s *stubRequestsTimer) StopTimers()    {}
func (s *stubRequestsTimer) RestartTimers() {}
func (s *stubRequestsTimer) RemoveRequest(types.RequestInfo) error {
	return nil
}

// stateRespondingComm answers the state transfer request of sync() on behalf of
// every other node with the view and sequence the cluster claims to be at.
type stateRespondingComm struct {
	stubComm
	collector *StateCollector
	view      uint64
	seq       uint64
}

func (c *stateRespondingComm) SendConsensus(targetID uint64, m *protos.Message) {
	if m.GetStateTransferRequest() == nil {
		return
	}

	c.collector.HandleMessage(targetID, &protos.Message{
		Content: &protos.Message_StateTransferResponse{
			StateTransferResponse: &protos.StateTransferResponse{
				ViewNum:  c.view,
				Sequence: c.seq,
			},
		},
	})
}

// syncFixture is a Controller whose sync() runs all the way to saving a record
// of the new view into a real WAL, together with the in-flight data that
// record's truncation flag is derived from.
type syncFixture struct {
	controller *Controller
	inFlight   *InFlightData
}

// newSyncFixture builds a controller that sits at controllerView and
// controllerSeq, and whose synchronizer returns a confirmed decision at syncSeq
// and syncView. The state transfer request is answered with a view one higher
// than syncView and syncSeq+1 as its sequence, which is what makes sync() save a
// record of the new view.
func newSyncFixture(t *testing.T, writeAheadLog api.WriteAheadLog, controllerSeq, controllerView, syncSeq, syncView uint64) *syncFixture {
	t.Helper()

	log := newQuietLogger()

	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{
		Metadata: MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence:  controllerSeq,
			ViewId:          controllerView,
			DecisionsInView: 1,
		}),
	}, []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}})

	collector := &StateCollector{
		SelfID:         1,
		N:              4,
		Logger:         log,
		CollectTimeout: 5 * time.Second,
	}
	collector.Start()
	t.Cleanup(collector.Stop)

	viewChanger := &ViewChanger{
		SelfID:              1,
		N:                   4,
		NodesList:           []uint64{1, 2, 3, 4},
		Logger:              log,
		Comm:                &stubComm{},
		RequestsTimer:       &stubRequestsTimer{},
		Ticker:              make(chan time.Time),
		InMsqQSize:          10,
		ControllerStartedWG: sync.WaitGroup{},
		MetricsViewChange:   api.NewMetricsViewChange(&disabled.Provider{}),
	}
	viewChanger.Start(controllerView)
	t.Cleanup(viewChanger.Stop)

	inFlight := &InFlightData{}

	controller := &Controller{
		ID:        1,
		N:         4,
		NodesList: []uint64{1, 2, 3, 4},
		Logger:    log,
		Comm: &stateRespondingComm{
			collector: collector,
			view:      syncView + 1,
			seq:       syncSeq + 1,
		},
		Synchronizer: &stubSynchronizer{response: types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence: syncSeq,
						ViewId:         syncView,
					}),
				},
				Signatures: []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		}},
		Checkpoint:  checkpoint,
		Collector:   collector,
		InFlight:    inFlight,
		ViewChanger: viewChanger,
		State:       &PersistedState{InFlightProposal: inFlight, Logger: log, WAL: writeAheadLog},
		WAL:         writeAheadLog,
	}
	controller.syncChan = make(chan struct{}, 1)

	return &syncFixture{controller: controller, inFlight: inFlight}
}

// newQuietLogger keeps the test output readable: the WAL and the controller both
// log a lot per record.
func newQuietLogger() api.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return logger.Sugar()
}

// mustNoError fails the test right away when there is an error to report.
func mustNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// mustNotNil fails the test right away when there is nothing to look at.
func mustNotNil(t *testing.T, object any, message string) {
	t.Helper()

	if object == nil {
		t.Fatalf("%s", message)
	}
}

// storeInFlightProposal makes the controller hold a proposal at the given view
// and sequence, the way a node that proposed before losing the leader does.
func (f *syncFixture) storeInFlightProposal(view, seq uint64) {
	f.inFlight.StoreProposal(types.Proposal{
		Metadata: MarshalOrPanic(&protos.ViewMetadata{
			ViewId:         view,
			LatestSequence: seq,
		}),
	})
}

// lastLogRecord returns the last record written into the WAL file with the
// given index. The CRC anchor is consumed by the reader and is not returned.
func lastLogRecord(t *testing.T, log api.Logger, dir string, index uint64) *protos.LogRecord {
	t.Helper()

	reader, err := wal.NewLogRecordReader(log, filepath.Join(dir, fmt.Sprintf("%016x.wal", index)))
	mustNoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	var last *protos.LogRecord
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		mustNoError(t, err)

		last = record
	}
	mustNotNil(t, last, "expected at least one record in the WAL")

	return last
}

// walFileNames lists the files of the WAL directory.
func walFileNames(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	mustNoError(t, err)

	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	return names
}

type abortRecordingProposer struct {
	aborted   bool
	abortChan chan struct{}
}

func (p *abortRecordingProposer) Propose(types.Proposal) {}
func (p *abortRecordingProposer) Start()                 {}

func (p *abortRecordingProposer) Abort() {
	p.aborted = true
	if p.abortChan != nil {
		close(p.abortChan)
	}
}

func (p *abortRecordingProposer) Stopped() bool              { return false }
func (p *abortRecordingProposer) AbortChan() <-chan struct{} { return p.abortChan }
func (p *abortRecordingProposer) GetLeaderID() uint64        { return 0 }
func (p *abortRecordingProposer) GetMetadata() []byte        { return nil }
func (p *abortRecordingProposer) HandleMessage(uint64, *protos.Message) {
}

type appendObservingWAL struct {
	api.WriteAheadLog
	observe func()
}

func (w *appendObservingWAL) Append(entry []byte, truncateTo bool) error {
	w.observe()
	return w.WriteAheadLog.Append(entry, truncateTo)
}

// TestSyncAbortsViewBeforeSavingNewView covers the write side of the truncation
// race: once the record of the new view is written, the view that was running
// when the sync started must not append anything after it.
func TestSyncAbortsViewBeforeSavingNewView(t *testing.T) {
	const (
		controllerSeq  = uint64(10)
		controllerView = uint64(5)
		inFlightSeq    = uint64(10)
		syncSeq        = uint64(12)
		syncView       = uint64(6)
	)

	dir := t.TempDir()
	log := newQuietLogger()
	writeAheadLog, err := wal.Create(log, dir, nil)
	mustNoError(t, err)
	defer func() {
		require.NoError(t, writeAheadLog.Close())
	}()

	proposer := &abortRecordingProposer{}
	savedAfterAbort := true
	observedWAL := &appendObservingWAL{
		WriteAheadLog: writeAheadLog,
		observe: func() {
			if !proposer.aborted {
				savedAfterAbort = false
			}
		},
	}

	fixture := newSyncFixture(t, observedWAL, controllerSeq, controllerView, syncSeq, syncView)
	fixture.storeInFlightProposal(controllerView, inFlightSeq)
	fixture.controller.currView = proposer

	fixture.controller.sync()

	require.True(t, proposer.aborted, "the view was not aborted before the new view was saved")
	require.True(t, savedAfterAbort, "the new view was saved before the view stopped")

	record := lastLogRecord(t, log, dir, 1)
	saved := &protos.SavedMessage{}
	mustNoError(t, proto.Unmarshal(record.GetData(), saved))
	mustNotNil(t, saved.GetNewView(), fmt.Sprintf("expected a record of the new view, got %v", saved))
}

// TestDecideDoesNotBlockOnAbortedView covers the hand-off between the view and
// the controller: a view that is being aborted must not stay blocked on a full
// decision channel, or the abort would wait for the view forever.
func TestDecideDoesNotBlockOnAbortedView(t *testing.T) {
	proposer := &abortRecordingProposer{abortChan: make(chan struct{})}
	controller := &Controller{
		currView:     proposer,
		decisionChan: make(chan decision, 1),
		stopChan:     make(chan struct{}),
	}
	controller.decisionChan <- decision{}

	decided := make(chan struct{})
	go func() {
		controller.Decide(types.Proposal{}, nil, nil)
		close(decided)
	}()

	select {
	case <-decided:
		t.Fatal("Decide returned before the view was aborted")
	case <-time.After(50 * time.Millisecond):
	}

	close(proposer.abortChan)

	select {
	case <-decided:
	case <-time.After(time.Second):
		t.Fatal("Decide did not return after the view was aborted")
	}
}

// TestSyncSavesNewViewWithTruncateWhenInFlightIsPruned covers R01: a node that
// proposed sequence 10 and then caught up with a cluster that is at sequence 12
// has nothing in flight once the sync is accounted for, so the record of the new
// view is the point from which everything older may go.
func TestSyncSavesNewViewWithTruncateWhenInFlightIsPruned(t *testing.T) {
	const (
		controllerSeq  = uint64(10)
		controllerView = uint64(3)
		inFlightSeq    = uint64(10)
		syncSeq        = uint64(12)
		syncView       = uint64(4)
	)

	dir := t.TempDir()
	log := newQuietLogger()
	writeAheadLog, err := wal.Create(log, dir, nil)
	mustNoError(t, err)
	defer func() {
		require.NoError(t, writeAheadLog.Close())
	}()

	fixture := newSyncFixture(t, writeAheadLog, controllerSeq, controllerView, syncSeq, syncView)
	fixture.storeInFlightProposal(controllerView, inFlightSeq)

	fixture.controller.sync()

	require.Nil(t, fixture.inFlight.InFlightProposal(), "in-flight proposal at sequence %d is stale once the cluster is at %d", inFlightSeq, syncSeq)

	record := lastLogRecord(t, log, dir, 1)
	require.True(t, record.GetTruncateTo(), "the record of the new view must be a truncation point, files: %v", walFileNames(t, dir))

	saved := &protos.SavedMessage{}
	mustNoError(t, proto.Unmarshal(record.GetData(), saved))
	newView := saved.GetNewView()
	mustNotNil(t, newView, fmt.Sprintf("expected a record of the new view, got %v", saved))
	require.Equal(t, syncView+1, newView.GetViewId())
	require.Equal(t, syncSeq, newView.GetLatestSequence())
}

// TestSyncSavesNewViewWithoutTruncateWhenInFlightSurvives covers R01.1: the sync
// moved the checkpoint to sequence 12, but the node still holds a proposal at
// sequence 13, which the cluster has not seen. Losing the records before the new
// view would cost the node the pre-prepare of its own proposal.
func TestSyncSavesNewViewWithoutTruncateWhenInFlightSurvives(t *testing.T) {
	const (
		controllerSeq  = uint64(10)
		controllerView = uint64(3)
		inFlightSeq    = uint64(13)
		syncSeq        = uint64(12)
		syncView       = uint64(4)
	)

	dir := t.TempDir()
	log := newQuietLogger()
	writeAheadLog, err := wal.Create(log, dir, nil)
	mustNoError(t, err)
	defer func() {
		require.NoError(t, writeAheadLog.Close())
	}()

	fixture := newSyncFixture(t, writeAheadLog, controllerSeq, controllerView, syncSeq, syncView)
	fixture.storeInFlightProposal(controllerView, inFlightSeq)

	fixture.controller.sync()

	inFlight := fixture.inFlight.InFlightProposal()
	mustNotNil(t, inFlight, fmt.Sprintf("the in-flight proposal at sequence %d outlives a sync to %d", inFlightSeq, syncSeq))
	inFlightMD := &protos.ViewMetadata{}
	mustNoError(t, proto.Unmarshal(inFlight.Metadata, inFlightMD))
	require.Equal(t, inFlightSeq, inFlightMD.GetLatestSequence())

	record := lastLogRecord(t, log, dir, 1)
	require.False(t, record.GetTruncateTo(), "a record written with a proposal in flight may not be a truncation point, files: %v", walFileNames(t, dir))

	saved := &protos.SavedMessage{}
	mustNoError(t, proto.Unmarshal(record.GetData(), saved))
	mustNotNil(t, saved.GetNewView(), fmt.Sprintf("expected a record of the new view, got %v", saved))
}

// TestSyncWALFileCountDoesNotGrowAcrossRestarts reproduces what an operator sees
// on a node that only ever syncs: every restart makes the WAL move to a new file
// and, absent a truncation point, nothing is ever freed. Each round the node
// holds a stale in-flight proposal and catches up with a cluster one sequence
// ahead, which is exactly the case where the record of the new view may truncate.
func TestSyncWALFileCountDoesNotGrowAcrossRestarts(t *testing.T) {
	const (
		rounds         = 5
		controllerView = uint64(3)
		firstSyncSeq   = uint64(12)
	)

	dir := t.TempDir()
	log := newQuietLogger()
	// A file size no record will ever reach, so the only rotations are the ones a
	// restart makes on its own.
	options := &wal.Options{FileSizeBytes: 64 * 1024 * 1024}

	writeAheadLog, err := wal.Create(log, dir, options)
	mustNoError(t, err)

	var counts []int
	for round := range rounds {
		if round > 0 {
			mustNoError(t, writeAheadLog.Close())

			writeAheadLog, err = wal.Open(log, dir, options)
			mustNoError(t, err)

			entries, err := writeAheadLog.ReadAll()
			mustNoError(t, err)
			require.Len(t, entries, 1, "only the record of the new view written by the previous round may be left to recover from")
		}
		defer func() {
			require.NoError(t, writeAheadLog.Close())
		}()

		syncSeq := firstSyncSeq + uint64(round)
		fixture := newSyncFixture(t, writeAheadLog, syncSeq-2, controllerView, syncSeq, controllerView+1)
		// A proposal the cluster has already moved past.
		fixture.storeInFlightProposal(controllerView, syncSeq-2)

		fixture.controller.sync()

		counts = append(counts, len(walFileNames(t, dir)))
	}

	require.Equal(t, counts[1], counts[rounds-1], "the number of WAL files must stop growing after the first restart, got %v", counts)
}

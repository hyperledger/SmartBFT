// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/pkg/wal"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// walLogFileNames lists the WAL files of a node directory, in index order.
func walLogFileNames(t *testing.T, dirPath string) []string {
	t.Helper()

	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".wal") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	return names
}

func walLogFileCount(t *testing.T, dirPath string) int {
	t.Helper()

	return len(walLogFileNames(t, dirPath))
}

// lastWalLogRecord reads the last record written to a node's WAL directory. It
// scans the files from the last index backwards, since ReadAll leaves a trailing
// file behind that may hold nothing but its CRC-anchor.
func lastWalLogRecord(t *testing.T, log api.Logger, dirPath string) *protos.LogRecord {
	t.Helper()

	names := walLogFileNames(t, dirPath)
	for _, name := range slices.Backward(names) {
		reader, err := wal.NewLogRecordReader(log, filepath.Join(dirPath, name))
		require.NoError(t, err)

		var last *protos.LogRecord
		for {
			record, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			last = record
		}
		require.NoError(t, reader.Close())

		if last != nil {
			return last
		}
	}

	t.Fatalf("no record found in the WAL directory %s", dirPath)

	return nil
}

func waitForSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitForPrepare(t *testing.T, prepares <-chan *protos.Message, what string) *protos.Message {
	t.Helper()

	select {
	case prepare := <-prepares:
		return prepare
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// TestRestartingNodeThatDoesNotProposeDoesNotGrowWAL covers a node in a network
// partition: it submits no requests and receives no messages from the other
// nodes. Each restart replays the WAL and starts consensus, but the node writes
// no records. The first restart brings the WAL to the state every following
// restart has to keep - the write head moves to a fresh file and nothing is
// written after it. Ten more restarts may not add files to it.
func TestRestartingNodeThatDoesNotProposeDoesNotGrowWAL(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	const numberOfNodes = 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		nodes = append(nodes, newNode(uint64(i), network, t.Name(), testDir, false, 0))
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	for _, n := range nodes {
		<-n.Delivered
	}

	restarting := nodes[numberOfNodes-1]
	restarting.Disconnect()

	walDir := filepath.Join(testDir, fmt.Sprintf("node%d", restarting.ID))

	// The node holds the records of the decision in its WAL, and it will write
	// nothing while it is in the partition. The first restart brings the WAL to
	// the steady state; the baseline is taken after it.
	restarting.Restart()
	baseline := walLogFileCount(t, walDir)
	require.Positive(t, baseline)

	for i := range 10 {
		restarting.Restart()
		require.LessOrEqual(t, walLogFileCount(t, walDir), baseline,
			"restart %d grew the WAL, baseline %d files", i+1, baseline)
	}

	require.Equal(t, baseline, walLogFileCount(t, walDir),
		"ten consecutive restarts must not grow the WAL")
}

// TestRestartKeepsViewAndInFlightProposal covers a node that persisted a
// proposal and crashed before collecting a quorum comes back to the same view
// with the same proposal in flight, ready to broadcast its prepare again. The
// node only ever accepts pre-prepares, so it can never decide on the proposal
// and stays with it in flight for the whole test.
func TestRestartKeepsViewAndInFlightProposal(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	const numberOfNodes = 4

	processedProposal := make(chan struct{}, 1)
	restoredProposal := make(chan struct{}, 1)
	originalPrepare := make(chan *protos.Message, 1)
	restoredPrepare := make(chan *protos.Message, 1)

	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		if i == numberOfNodes {
			n.LoseMessages(func(msg *protos.Message) bool {
				return msg.GetPrePrepare() == nil
			})

			logger := n.logger.Desugar()
			n.logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, "Processed proposal with seq") {
					select {
					case processedProposal <- struct{}{}:
					default:
					}
				}
				if strings.Contains(entry.Message, "Restored proposal with sequence") {
					select {
					case restoredProposal <- struct{}{}:
					default:
					}
				}
				return nil
			})).Sugar()
			n.Setup()
			n.MutateSend(1, func(_ uint64, msg *protos.Message) {
				if msg.GetPrepare() != nil {
					select {
					case originalPrepare <- msg:
					default:
					}
				}
			})
		}
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	waitForSignal(t, processedProposal, "the isolated node to persist the proposal")
	for i := range numberOfNodes - 1 {
		<-nodes[i].Delivered
	}
	isolated := nodes[numberOfNodes-1]
	walDir := filepath.Join(testDir, fmt.Sprintf("node%d", isolated.ID))

	recordBefore := lastWalLogRecord(t, isolated.logger, walDir)
	savedBefore := &protos.SavedMessage{}
	require.NoError(t, proto.Unmarshal(recordBefore.GetData(), savedBefore))
	prePrepare := savedBefore.GetProposedRecord().GetPrePrepare()
	require.NotNil(t, prePrepare, "the isolated node must have persisted the proposal")

	expectedProposal := types.Proposal{
		VerificationSequence: int64(prePrepare.GetProposal().GetVerificationSequence()),
		Header:               prePrepare.GetProposal().GetHeader(),
		Payload:              prePrepare.GetProposal().GetPayload(),
		Metadata:             prePrepare.GetProposal().GetMetadata(),
	}
	leaderBeforeRestart := isolated.Consensus.GetLeaderID()

	waitForPrepare(t, originalPrepare, "the isolated node to broadcast its prepare")

	// Watch for the prepare the restored view re-broadcasts for its in-flight
	// proposal. The original prepare is already behind us, so anything this
	// channel receives now came out of the recovery.
	isolated.MutateSend(1, func(_ uint64, msg *protos.Message) {
		if msg.GetPrepare() == nil {
			return
		}
		select {
		case restoredPrepare <- msg:
		default:
		}
	})

	isolated.Restart()

	waitForSignal(t, restoredProposal, "the node to restore the proposal in flight")

	prepare := waitForPrepare(t, restoredPrepare,
		"the restored node to re-broadcast a prepare for its in-flight proposal")
	require.Equal(t, expectedProposal.Digest(), prepare.GetPrepare().GetDigest(),
		"the re-broadcast prepare must be for the restored proposal")

	require.Equal(t, leaderBeforeRestart, isolated.Consensus.GetLeaderID(),
		"the node must come back to the same view")

	recordAfter := lastWalLogRecord(t, isolated.logger, walDir)
	savedAfter := &protos.SavedMessage{}
	require.NoError(t, proto.Unmarshal(recordAfter.GetData(), savedAfter))
	require.True(t, proto.Equal(savedBefore, savedAfter),
		"the WAL must still carry the same proposal in flight")
	// The first restart after a proposal moves the write head to a new file. From
	// then on ReadAll reuses that anchor-only file, so further restarts add none.
	// Sync is off for these restarts, so the count only reflects the WAL reopen.
	baseline := walLogFileCount(t, walDir)
	for i := range 10 {
		isolated.RestartSync(false)
		require.LessOrEqual(t, walLogFileCount(t, walDir), baseline,
			"restart %d grew the WAL, baseline %d files", i+1, baseline)
	}

	require.Equal(t, baseline, walLogFileCount(t, walDir),
		"ten consecutive restarts must not grow the WAL")
}

// TestNodeWithoutPrePrepareDoesNotGrowWAL covers the live-node variant of the
// WAL growth: the node never restarts, keeps receiving every consensus message
// except pre-prepares, and still goes through view changes. On main, its
// ViewChange and NewView records are not truncation points, so the WAL grows at
// every file rotation.
func TestNodeWithoutPrePrepareDoesNotGrowWAL(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	const numberOfNodes = 4

	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		var options []appOption
		if i == numberOfNodes {
			// A tiny file makes every view-change record rotate the WAL, so the
			// test does not have to write megabytes to observe the growth.
			options = append(options, WithWalOptions(&wal.Options{FileSizeBytes: 32}))
		}
		nodes = append(nodes, newNode(uint64(i), network, t.Name(), testDir, false, 0, options...))
	}

	isolated := nodes[numberOfNodes-1]
	isolated.LoseMessages(func(m *protos.Message) bool {
		return m.GetPrePrepare() != nil
	})

	startedViews := make(chan uint64, 16)
	processedProposal := make(chan struct{}, 1)
	baseLogger := isolated.Consensus.Logger.(*zap.SugaredLogger).Desugar()
	isolated.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Processed proposal with seq") {
			select {
			case processedProposal <- struct{}{}:
			default:
			}
		}
		var view, sequence, decisions uint64
		if _, err := fmt.Sscanf(entry.Message, "Starting view with number %d, sequence %d, and decisions %d", &view, &sequence, &decisions); err == nil {
			select {
			case startedViews <- view:
			default:
			}
		}
		return nil
	})).Sugar()

	startNodes(nodes, network)

	// The cluster keeps deciding while node 4 is alive but cannot process a
	// pre-prepare itself. Node 4 is allowed to be behind; it catches up via sync.
	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	for _, n := range nodes[:numberOfNodes-1] {
		<-n.Delivered
	}

	walDir := filepath.Join(testDir, fmt.Sprintf("node%d", isolated.ID))

	// The first view change moves the WAL to its steady state: the truncating
	// record keeps the file it was written to and the switch creates the next
	// one. The baseline is taken after it. Complaining on every node makes the
	// view change deterministic instead of waiting for the heartbeat timeout.
	for _, n := range nodes {
		n.Consensus.Complain(0, true)
	}
	waitForStartedView(t, startedViews, 1)
	baseline := walLogFileCount(t, walDir)

	// View 2 is led by node 3, so node 4 stays a follower and keeps losing
	// pre-prepares. Its WAL must not add a file for this view change.
	for _, n := range nodes {
		n.Consensus.Complain(1, true)
	}
	waitForStartedView(t, startedViews, 2)

	require.LessOrEqual(t, walLogFileCount(t, walDir), baseline,
		"a live node that misses pre-prepares must not grow its WAL on view changes")

	select {
	case <-processedProposal:
		t.Fatal("the isolated node accepted a pre-prepare")
	default:
	}

	lastRecord := lastWalLogRecord(t, isolated.logger, walDir)
	lastMessage := &protos.SavedMessage{}
	require.NoError(t, proto.Unmarshal(lastRecord.GetData(), lastMessage))
	require.NotNil(t, lastMessage.GetNewView(), "the node must have persisted the new view")
}

func waitForStartedView(t *testing.T, views <-chan uint64, target uint64) {
	t.Helper()

	timeout := time.After(30 * time.Second)
	for {
		select {
		case view := <-views:
			if view >= target {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for view %d", target)
		}
	}
}

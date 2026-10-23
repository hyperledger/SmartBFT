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

// TestRestartingNodeThatDoesNotProposeDoesNotGrowWAL covers R07.1 and R15i on
// real nodes: a node in a network partition neither proposes nor syncs, so its
// restarts are a crash loop. The first restart brings the WAL to the state every
// following restart has to keep - the write head moves to a fresh file and
// nothing is written after it. Ten more restarts may not add files to it.
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

// TestRestartKeepsViewAndInFlightProposal covers R15i: a node that persisted a
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
	filesBeforeRestart := walLogFileCount(t, walDir)

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
	require.LessOrEqual(t, walLogFileCount(t, walDir), filesBeforeRestart,
		"the restart added a WAL file")
}

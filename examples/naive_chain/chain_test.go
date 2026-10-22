// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"fmt"
	"testing"
	"time"

	smart "github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/wal"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestTxn(t *testing.T) {
	txn := &Transaction{
		ID:       "id",
		ClientID: "client",
	}
	rawTxn := txn.ToBytes()
	txn2 := TransactionFromBytes(rawTxn)
	require.Equal(t, txn, txn2)
}

func TestBlock(t *testing.T) {
	tx1 := &Transaction{
		ID:       "tx1",
		ClientID: "alice",
	}
	tx2 := &Transaction{
		ID:       "tx2",
		ClientID: "bob",
	}
	tx3 := &Transaction{
		ID:       "tx3",
		ClientID: "carol",
	}

	block := &BlockData{
		Transactions: [][]byte{
			tx1.ToBytes(),
			tx2.ToBytes(),
			tx3.ToBytes(),
		},
	}

	rawBlock := block.ToBytes()
	block2 := BlockDataFromBytes(rawBlock)
	require.Equal(t, block, block2)
}

func TestBlockHeader(t *testing.T) {
	blockHeader := &BlockHeader{
		PrevHash: "prev",
		Sequence: 2,
		DataHash: "hash",
	}

	headerBytes := blockHeader.ToBytes()
	blockHeader2 := BlockHeaderFromBytes(headerBytes)
	require.Equal(t, blockHeader, blockHeader2)
}

func TestChain(t *testing.T) {
	blockCount := 10

	testDir := t.TempDir()

	numNodes := 4
	chains := setupNetwork(t, NetworkOptions{NumNodes: numNodes, BatchSize: 1, BatchTimeout: 10 * time.Second}, testDir)

	for blockSeq := 1; blockSeq < blockCount; blockSeq++ {
		err := chains[1].Order(Transaction{
			ClientID: "alice",
			ID:       fmt.Sprintf("tx%d", blockSeq),
		})
		require.NoError(t, err)
		for i := 1; i <= numNodes; i++ {
			chain := chains[i]
			block := chain.Listen()
			require.Equal(t, uint64(blockSeq), block.Sequence)
			require.Equal(t, []Transaction{{ID: fmt.Sprintf("tx%d", blockSeq), ClientID: "alice"}}, block.Transactions)
		}
	}

	for _, chain := range chains {
		chain.node.Stop()
	}
}

func setupNode(t *testing.T, id int, opt NetworkOptions, network map[int]map[int]chan proto.Message, testDir string) *Chain {
	ingress := make(Ingress)
	for from := 1; from <= opt.NumNodes; from++ {
		ingress[from] = network[id][from]
	}

	egress := make(Egress)
	for to := 1; to <= opt.NumNodes; to++ {
		egress[to] = network[to][id]
	}

	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	logger := basicLog.Sugar()
	met := &disabled.Provider{}
	walMet := wal.NewMetrics(met, "label1")
	bftMet := smart.NewMetrics(met, "label1")

	chain := NewChain(uint64(id), ingress, egress, logger, walMet, bftMet, opt, testDir)

	return chain
}

func setupNetwork(t *testing.T, opt NetworkOptions, testDir string) map[int]*Chain {
	network := make(map[int]map[int]chan proto.Message)

	chains := make(map[int]*Chain)

	for id := 1; id <= opt.NumNodes; id++ {
		network[id] = make(map[int]chan proto.Message)
		for i := 1; i <= opt.NumNodes; i++ {
			network[id][i] = make(chan proto.Message, 128)
		}
	}

	for id := 1; id <= opt.NumNodes; id++ {
		chains[id] = setupNode(t, id, opt, network, testDir)
	}
	return chains
}

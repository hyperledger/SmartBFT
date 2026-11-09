// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"encoding/asn1"

	smart "github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/wal"
)

type Block struct {
	Sequence     uint64
	PrevHash     string
	Transactions []Transaction
}

type BlockHeader struct {
	Sequence int64
	PrevHash string
	DataHash string
}

func (header BlockHeader) ToBytes() []byte {
	rawHeader, err := asn1.Marshal(header)
	if err != nil {
		panic(err)
	}
	return rawHeader
}

func BlockHeaderFromBytes(rawHeader []byte) *BlockHeader {
	var header BlockHeader
	asn1.Unmarshal(rawHeader, &header)
	return &header
}

type Transaction struct {
	ClientID string
	ID       string
}

func (txn Transaction) ToBytes() []byte {
	rawTxn, err := asn1.Marshal(txn)
	if err != nil {
		panic(err)
	}
	return rawTxn
}

func TransactionFromBytes(rawTxn []byte) *Transaction {
	var txn Transaction
	asn1.Unmarshal(rawTxn, &txn)
	return &txn
}

type BlockData struct {
	Transactions [][]byte
}

func (b BlockData) ToBytes() []byte {
	rawBlock, err := asn1.Marshal(b)
	if err != nil {
		panic(err)
	}
	return rawBlock
}

func BlockDataFromBytes(rawBlock []byte) *BlockData {
	var block BlockData
	asn1.Unmarshal(rawBlock, &block)
	return &block
}

type Chain struct {
	deliverChan <-chan *Block
	node        *Node
}

func NewChain(id uint64, in Ingress, out Egress, logger smart.Logger, walmet *wal.Metrics, bftmet *smart.Metrics, opts NetworkOptions, testDir string) *Chain {
	deliverChan := make(chan *Block)
	node := NewNode(id, in, out, deliverChan, logger, walmet, bftmet, opts, testDir)
	return &Chain{
		node:        node,
		deliverChan: deliverChan,
	}
}

func (chain *Chain) Listen() Block {
	block := <-chain.deliverChan
	return *block
}

func (chain *Chain) Order(txn Transaction) error {
	return chain.node.consensus.SubmitRequest(txn.ToBytes())
}

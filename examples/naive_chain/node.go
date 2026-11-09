// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	smart "github.com/hyperledger/SmartBFT/pkg/api"
	smartbft "github.com/hyperledger/SmartBFT/pkg/consensus"
	bft "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/pkg/wal"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type (
	Ingress map[int]<-chan proto.Message
	Egress  map[int]chan<- proto.Message
)

type NetworkOptions struct {
	NumNodes     int
	BatchSize    uint64
	BatchTimeout time.Duration
}

type Node struct {
	clock       *time.Ticker
	secondClock *time.Ticker
	stopChan    chan struct{}
	doneWG      sync.WaitGroup
	prevHash    string
	id          uint64
	in          Ingress
	out         Egress
	deliverChan chan<- *Block
	consensus   *smartbft.Consensus
}

func (*Node) Sync() bft.SyncResponse {
	panic("implement me")
}

func (*Node) AuxiliaryData(_ []byte) []byte {
	return nil
}

func (*Node) RequestID(req []byte) bft.RequestInfo {
	txn := TransactionFromBytes(req)
	return bft.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

func (*Node) VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error) {
	blockData := BlockDataFromBytes(proposal.Payload)
	requests := make([]bft.RequestInfo, 0)
	for _, t := range blockData.Transactions {
		tx := TransactionFromBytes(t)
		reqInfo := bft.RequestInfo{ID: tx.ID, ClientID: tx.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

func (*Node) RequestsFromProposal(proposal bft.Proposal) []bft.RequestInfo {
	blockData := BlockDataFromBytes(proposal.Payload)
	requests := make([]bft.RequestInfo, 0)
	for _, t := range blockData.Transactions {
		tx := TransactionFromBytes(t)
		reqInfo := bft.RequestInfo{ID: tx.ID, ClientID: tx.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests
}

func (*Node) VerifyRequest(val []byte) (bft.RequestInfo, error) {
	return bft.RequestInfo{}, nil
}

func (*Node) VerifyConsenterSig(_ bft.Signature, prop bft.Proposal) ([]byte, error) {
	return nil, nil
}

func (*Node) VerifySignature(signature bft.Signature) error {
	return nil
}

func (*Node) VerificationSequence() uint64 {
	return 0
}

func (*Node) Sign(msg []byte) []byte {
	return nil
}

func (n *Node) SignProposal(bft.Proposal, []byte) *bft.Signature {
	return &bft.Signature{
		ID: n.id,
	}
}

func (n *Node) AssembleProposal(metadata []byte, requests [][]byte) bft.Proposal {
	blockData := BlockData{Transactions: requests}.ToBytes()
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(fmt.Sprintf("Unable to unmarshal metadata, error: %v", err))
	}
	return bft.Proposal{
		Header: BlockHeader{
			PrevHash: n.prevHash,
			DataHash: computeDigest(blockData),
			Sequence: int64(md.GetLatestSequence()),
		}.ToBytes(),
		Payload:  BlockData{Transactions: requests}.ToBytes(),
		Metadata: metadata,
	}
}

func (n *Node) SendConsensus(targetID uint64, message *smartbftprotos.Message) {
	n.out[int(targetID)] <- message
}

func (n *Node) SendTransaction(targetID uint64, request []byte) {
	msg := &FwdMessage{
		Payload: request,
		Sender:  n.id,
	}
	n.out[int(targetID)] <- msg
}

func (n *Node) MembershipChange() bool {
	return false
}

func (n *Node) Deliver(proposal bft.Proposal, signature []bft.Signature) bft.Reconfig {
	blockData := BlockDataFromBytes(proposal.Payload)
	txns := make([]Transaction, 0, len(blockData.Transactions))
	for _, rawTxn := range blockData.Transactions {
		txn := TransactionFromBytes(rawTxn)
		txns = append(txns, Transaction{
			ClientID: txn.ClientID,
			ID:       txn.ID,
		})
	}
	header := BlockHeaderFromBytes(proposal.Header)

	select {
	case <-n.stopChan:
		return bft.Reconfig{InLatestDecision: false}
	case n.deliverChan <- &Block{
		Sequence:     uint64(header.Sequence),
		PrevHash:     header.PrevHash,
		Transactions: txns,
	}:
	}

	return bft.Reconfig{InLatestDecision: false}
}

func NewNode(id uint64, in Ingress, out Egress, deliverChan chan<- *Block, logger smart.Logger, walmet *wal.Metrics, bftmet *smart.Metrics, opts NetworkOptions, testDir string) *Node {
	nodeDir := filepath.Join(testDir, fmt.Sprintf("node%d", id))

	writeAheadLog, err := wal.Create(logger, nodeDir, &wal.Options{Metrics: walmet.With("label1", "val1")})
	if err != nil {
		logger.Panicf("Cannot create WAL at %s", nodeDir)
	}

	node := &Node{
		clock:       time.NewTicker(time.Second),
		secondClock: time.NewTicker(time.Second),
		id:          id,
		in:          in,
		out:         out,
		deliverChan: deliverChan,
		stopChan:    make(chan struct{}),
	}

	config := bft.DefaultConfig
	config.SelfID = id
	config.RequestBatchMaxInterval = opts.BatchTimeout
	config.RequestBatchMaxCount = opts.BatchSize

	node.consensus = &smartbft.Consensus{
		Config:             config,
		ViewChangerTicker:  node.secondClock.C,
		Scheduler:          node.clock.C,
		Logger:             logger,
		Metrics:            bftmet,
		Comm:               node,
		Signer:             node,
		MembershipNotifier: node,
		Verifier:           node,
		Application:        node,
		Assembler:          node,
		RequestInspector:   node,
		Synchronizer:       node,
		WAL:                writeAheadLog,
		Metadata: &smartbftprotos.ViewMetadata{
			LatestSequence: 0,
			ViewId:         0,
		},
	}
	if err = node.consensus.Start(); err != nil {
		panic("error on consensus start")
	}
	node.Start()
	return node
}

func (n *Node) Start() {
	for id, in := range n.in {
		if uint64(id) == n.id {
			continue
		}
		n.doneWG.Add(1)

		go func(id uint64, in <-chan proto.Message) {
			defer n.doneWG.Done()

			for {
				select {
				case <-n.stopChan:
					return
				case msg := <-in:
					switch m := msg.(type) {
					case *smartbftprotos.Message:
						n.consensus.HandleMessage(id, m)
					case *FwdMessage:
						n.consensus.SubmitRequest(m.GetPayload())
					}
				}
			}
		}(uint64(id), in)
	}
}

func (n *Node) Stop() {
	select {
	case <-n.stopChan:
		break
	default:
		close(n.stopChan)
	}
	n.clock.Stop()
	n.doneWG.Wait()
	n.consensus.Stop()
}

func (n *Node) Nodes() []uint64 {
	nodes := make([]uint64, 0, len(n.in))
	for id := range n.in {
		nodes = append(nodes, uint64(id))
	}

	return nodes
}

func computeDigest(rawBytes []byte) string {
	h := sha256.New()
	h.Write(rawBytes)
	digest := h.Sum(nil)
	return hex.EncodeToString(digest)
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type bftNode struct {
	id         uint64
	membership membership
	publicKeys map[uint64]ed25519.PublicKey
	privateKey ed25519.PrivateKey
	core       *coreClient
	logger     api.Logger
	trace      *traceStore
	comm       *httpComm
	consensus  *consensus.Consensus
	clock      *time.Ticker
	viewClock  *time.Ticker
	mu         sync.RWMutex
	latest     types.Decision
	started    atomic.Bool
	stopOnce   sync.Once
}

const (
	mwvnPrepareComplaintGrace  = 30 * time.Second
	mwvnPrepareAutoRemoveGrace = time.Minute
)

// mwvnConsensusConfig keeps the generic SmartBFT defaults unchanged while
// aligning the MWVN adapter's request lifecycle with its long PREPARE window.
// The engine-level active-buffer guard remains the final authority when a
// request timer races with an in-flight proposal.
func mwvnConsensusConfig(id uint64, prepareTimeout time.Duration) types.Configuration {
	config := types.DefaultConfig
	config.SelfID = id
	config.RequestBatchMaxCount = 1
	config.RequestBatchMaxBytes = 1024 * 1024
	config.RequestBatchMaxInterval = 25 * time.Millisecond
	config.RequestMaxBytes = 64 * 1024
	config.RequestPoolSize = 100
	config.SyncOnStart = false
	config.LeaderRotation = false
	config.DecisionsPerLeader = 0
	config.PrepareVoteCollectionTimeout = prepareTimeout
	config.RequestTimerGuardDuringPrepare = true
	config.RequestComplainTimeout = prepareTimeout + mwvnPrepareComplaintGrace
	config.RequestAutoRemoveTimeout = prepareTimeout + mwvnPrepareAutoRemoveGrace
	return config
}

func newBFTNode(id uint64, config membership, publicKeys map[uint64]ed25519.PublicKey, privateKey ed25519.PrivateKey, coreURL, dataDir string, prepareTimeout time.Duration, logger api.Logger) (*bftNode, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create node data directory: %w", err)
	}
	core := newCoreClient(coreURL)
	state, err := core.state()
	if err != nil {
		return nil, err
	}
	if state.NodeID != id {
		return nil, fmt.Errorf("Python core node ID %d does not match engine ID %d", state.NodeID, id)
	}
	if state.Height != 0 {
		return nil, errors.New("restart from an existing application ledger is not implemented; use a fresh demo data directory")
	}
	provider := &disabled.Provider{}
	writeAheadLog, initialEntries, err := wal.InitializeAndReadAll(logger, filepath.Join(dataDir, "wal"), &wal.Options{Metrics: wal.NewMetrics(provider, "bftnode").With(fmt.Sprint(id))})
	if err != nil {
		return nil, fmt.Errorf("initialize WAL: %w", err)
	}
	if len(initialEntries) != 0 {
		return nil, errors.New("restart from an existing SmartBFT WAL is not implemented; run make demo-clean")
	}
	trace := &traceStore{}
	comm := newHTTPComm(id, config, privateKey, trace)
	node := &bftNode{
		id:         id,
		membership: config,
		publicKeys: publicKeys,
		privateKey: privateKey,
		core:       core,
		logger:     logger,
		trace:      trace,
		comm:       comm,
		clock:      time.NewTicker(time.Second),
		viewClock:  time.NewTicker(time.Second),
	}
	consensusConfig := mwvnConsensusConfig(id, prepareTimeout)
	node.consensus = &consensus.Consensus{
		Config:             consensusConfig,
		ViewChangerTicker:  node.viewClock.C,
		Scheduler:          node.clock.C,
		Logger:             logger,
		Metrics:            api.NewMetrics(provider, "bftnode").With(fmt.Sprint(id)),
		Comm:               comm,
		Signer:             node,
		MembershipNotifier: node,
		Verifier:           node,
		Application:        node,
		Assembler:          node,
		RequestInspector:   node,
		Synchronizer:       node,
		WAL:                writeAheadLog,
		WALInitialContent:  initialEntries,
		Metadata:           &smartbftprotos.ViewMetadata{},
	}
	return node, nil
}

func (n *bftNode) start() error {
	if err := n.consensus.Start(); err != nil {
		return err
	}
	n.started.Store(true)
	return nil
}

func (n *bftNode) stop() {
	n.stopOnce.Do(func() {
		if n.started.Load() {
			n.consensus.Stop()
		}
		n.clock.Stop()
		n.viewClock.Stop()
	})
}

func (n *bftNode) Sync() types.SyncResponse {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return types.SyncResponse{Latest: n.latest}
}

func (*bftNode) AuxiliaryData(message []byte) []byte {
	return append([]byte(nil), message...)
}

func (n *bftNode) RequestID(raw []byte) types.RequestInfo {
	return types.RequestInfo{ClientID: "mwvn-approved-qc", ID: requestID(raw)}
}

func (n *bftNode) VerifyRequest(raw []byte) (types.RequestInfo, error) {
	id, err := n.core.verifyRequest(raw)
	if err != nil {
		return types.RequestInfo{}, err
	}
	return types.RequestInfo{ClientID: "mwvn-approved-qc", ID: id}, nil
}

type proposalVerificationRequest struct {
	Header      proposalHeader    `json:"header"`
	ApprovedQCs []json.RawMessage `json:"approved_qcs"`
}

func parseProposal(proposal types.Proposal) (proposalHeader, proposalPayload, *smartbftprotos.ViewMetadata, error) {
	var header proposalHeader
	if err := decodeStrictJSON(proposal.Header, &header); err != nil {
		return header, proposalPayload{}, nil, fmt.Errorf("decode proposal header: %w", err)
	}
	if header.SchemaVersion != proposalSchema {
		return header, proposalPayload{}, nil, errors.New("invalid proposal schema_version")
	}
	var payload proposalPayload
	if err := decodeStrictJSON(proposal.Payload, &payload); err != nil {
		return header, payload, nil, fmt.Errorf("decode proposal payload: %w", err)
	}
	if len(payload.ApprovedQCs) == 0 {
		return header, payload, nil, errors.New("proposal contains no approved QCs")
	}
	if header.DataHash != digestBytes(proposal.Payload) {
		return header, payload, nil, errors.New("proposal data hash mismatch")
	}
	metadata := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, metadata); err != nil {
		return header, payload, nil, fmt.Errorf("decode SmartBFT metadata: %w", err)
	}
	if header.Sequence != metadata.LatestSequence {
		return header, payload, nil, errors.New("proposal sequence does not match SmartBFT metadata")
	}
	return header, payload, metadata, nil
}

func (n *bftNode) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	header, payload, _, err := parseProposal(proposal)
	if err != nil {
		return nil, err
	}
	raw, err := canonicalJSON(proposalVerificationRequest{Header: header, ApprovedQCs: payload.ApprovedQCs})
	if err != nil {
		return nil, err
	}
	ids, err := n.core.verifyProposal(raw)
	if err != nil {
		return nil, err
	}
	if len(ids) != len(payload.ApprovedQCs) {
		return nil, errors.New("Python core returned the wrong number of request identities")
	}
	result := make([]types.RequestInfo, 0, len(ids))
	for _, id := range ids {
		result = append(result, types.RequestInfo{ClientID: "mwvn-approved-qc", ID: id})
	}
	return result, nil
}

func (n *bftNode) RequestsFromProposal(proposal types.Proposal) []types.RequestInfo {
	_, payload, _, err := parseProposal(proposal)
	if err != nil {
		return nil
	}
	result := make([]types.RequestInfo, 0, len(payload.ApprovedQCs))
	for _, raw := range payload.ApprovedQCs {
		result = append(result, types.RequestInfo{ClientID: "mwvn-approved-qc", ID: requestID(raw)})
	}
	return result
}

func (*bftNode) VerificationSequence() uint64 {
	return 0
}

func (n *bftNode) Sign(message []byte) []byte {
	return ed25519.Sign(n.privateKey, signaturePayload("smartbft-view-data/v1", "", message))
}

func (n *bftNode) SignProposal(proposal types.Proposal, auxiliary []byte) *types.Signature {
	return &types.Signature{ID: n.id, Value: ed25519.Sign(n.privateKey, signaturePayload("smartbft-proposal/v1", proposal.Digest(), auxiliary)), Msg: append([]byte(nil), auxiliary...)}
}

func (n *bftNode) VerifyConsenterSig(signature types.Signature, proposal types.Proposal) ([]byte, error) {
	publicKey, exists := n.publicKeys[signature.ID]
	if !exists {
		return nil, fmt.Errorf("unknown validator %d", signature.ID)
	}
	if !ed25519.Verify(publicKey, signaturePayload("smartbft-proposal/v1", proposal.Digest(), signature.Msg), signature.Value) {
		return nil, errors.New("invalid validator proposal signature")
	}
	return append([]byte(nil), signature.Msg...), nil
}

func (n *bftNode) VerifySignature(signature types.Signature) error {
	publicKey, exists := n.publicKeys[signature.ID]
	if !exists {
		return fmt.Errorf("unknown validator %d", signature.ID)
	}
	if !ed25519.Verify(publicKey, signaturePayload("smartbft-view-data/v1", "", signature.Msg), signature.Value) {
		return errors.New("invalid validator view-data signature")
	}
	return nil
}

func signaturePayload(domain, digest string, message []byte) []byte {
	result := make([]byte, 0, len(domain)+len(digest)+len(message)+24)
	for _, field := range [][]byte{[]byte(domain), []byte(digest), message} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		result = append(result, length[:]...)
		result = append(result, field...)
	}
	return result
}

func (n *bftNode) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	state, err := n.core.state()
	if err != nil {
		n.logger.Panicf("read Python core state: %v", err)
	}
	payload := proposalPayload{ApprovedQCs: make([]json.RawMessage, 0, len(requests))}
	for _, request := range requests {
		copy := append(json.RawMessage(nil), request...)
		payload.ApprovedQCs = append(payload.ApprovedQCs, copy)
	}
	payloadBytes, err := canonicalJSON(payload)
	if err != nil {
		n.logger.Panicf("encode proposal payload: %v", err)
	}
	viewMetadata := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, viewMetadata); err != nil {
		n.logger.Panicf("decode proposal metadata: %v", err)
	}
	previousHash := state.LedgerHead
	if previousHash == "" {
		previousHash = fmt.Sprintf("%064x", 0)
	}
	header, err := canonicalJSON(proposalHeader{SchemaVersion: proposalSchema, Sequence: viewMetadata.LatestSequence, PreviousHash: previousHash, DataHash: digestBytes(payloadBytes)})
	if err != nil {
		n.logger.Panicf("encode proposal header: %v", err)
	}
	return types.Proposal{Header: header, Payload: payloadBytes, Metadata: metadata}
}

func (*bftNode) MembershipChange() bool {
	return false
}

type proofSignature struct {
	ValidatorID uint64 `json:"validator_id"`
	Value       string `json:"value"`
	Auxiliary   string `json:"auxiliary"`
}

type commitRequest struct {
	SchemaVersion  string            `json:"schema_version"`
	NodeID         uint64            `json:"node_id"`
	Sequence       uint64            `json:"sequence"`
	View           uint64            `json:"view"`
	PreviousHash   string            `json:"previous_hash"`
	ProposalDigest string            `json:"proposal_digest"`
	ApprovedQCs    []json.RawMessage `json:"approved_qcs"`
	DecisionProof  []proofSignature  `json:"decision_proof"`
}

func (n *bftNode) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	header, payload, metadata, err := parseProposal(proposal)
	if err != nil {
		n.logger.Panicf("deliver invalid proposal: %v", err)
	}
	quorum := 2*((len(n.membership.Nodes)-1)/3) + 1
	if len(signatures) < quorum {
		n.logger.Panicf("decision proof has %d signatures, need %d", len(signatures), quorum)
	}
	seen := make(map[uint64]struct{}, len(signatures))
	proof := make([]proofSignature, 0, len(signatures))
	for _, signature := range signatures {
		if _, duplicate := seen[signature.ID]; duplicate {
			n.logger.Panicf("decision proof repeats validator %d", signature.ID)
		}
		seen[signature.ID] = struct{}{}
		if _, err := n.VerifyConsenterSig(signature, proposal); err != nil {
			n.logger.Panicf("invalid decision proof: %v", err)
		}
		proof = append(proof, proofSignature{ValidatorID: signature.ID, Value: base64.StdEncoding.EncodeToString(signature.Value), Auxiliary: base64.StdEncoding.EncodeToString(signature.Msg)})
	}
	raw, err := canonicalJSON(commitRequest{SchemaVersion: "mwvn-bft-commit/v1", NodeID: n.id, Sequence: header.Sequence, View: metadata.ViewId, PreviousHash: header.PreviousHash, ProposalDigest: proposal.Digest(), ApprovedQCs: payload.ApprovedQCs, DecisionProof: proof})
	if err != nil {
		n.logger.Panicf("encode commit callback: %v", err)
	}
	if err := n.core.commit(raw); err != nil {
		n.logger.Panicf("Python core rejected commit: %v", err)
	}
	n.mu.Lock()
	n.latest = types.Decision{Proposal: proposal, Signatures: signatures}
	n.mu.Unlock()
	n.trace.add(traceEvent{NodeID: n.id, Direction: "commit", Sender: n.id, Receiver: n.id, MessageType: "LEDGER-APPEND", Sequence: &header.Sequence, ProposalDigest: proposal.Digest(), SizeBytes: len(raw), Outcome: "python_core_durable"})
	return types.Reconfig{InLatestDecision: false}
}

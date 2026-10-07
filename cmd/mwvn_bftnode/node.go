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
	dataDir    string
	deliveryMu sync.Mutex
}

// mwvnConsensusConfig configures the adapter without changing SmartBFT's
// ordinary protocol timers. Evidence-window timing belongs to the application
// state machine, not to the engine's PREPARE phase.
func mwvnConsensusConfig(id uint64) types.Configuration {
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
	return config
}

func newBFTNode(id uint64, config membership, publicKeys map[uint64]ed25519.PublicKey, privateKey ed25519.PrivateKey, coreURL, dataDir string, logger api.Logger) (*bftNode, error) {
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
	provider := &disabled.Provider{}
	writeAheadLog, initialEntries, err := wal.InitializeAndReadAll(logger, filepath.Join(dataDir, "wal"), &wal.Options{Metrics: wal.NewMetrics(provider, "bftnode").With(fmt.Sprint(id))})
	if err != nil {
		return nil, fmt.Errorf("initialize WAL: %w", err)
	}
	trace := &traceStore{}
	comm := newHTTPComm(id, config, privateKey, trace)
	node := &bftNode{
		id:         id,
		dataDir:    dataDir,
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
	consensusConfig := mwvnConsensusConfig(id)
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
	if err := node.restoreArchive(state.Height); err != nil {
		return nil, err
	}
	return node, nil
}

func (n *bftNode) start() error {
	// HTTP is already listening, so peers can serve certified history even
	// while all engines are recovering concurrently.
	if err := n.catchUp(); err != nil {
		return err
	}
	n.restoreCheckpoint()
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
	if err := n.catchUp(); err != nil {
		n.logger.Warnf("state catch-up: %v", err)
	}
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
	ApprovedQCs []json.RawMessage `json:"approved_qcs,omitempty"`
	Commands    []json.RawMessage `json:"commands,omitempty"`
}

func parseProposal(proposal types.Proposal) (proposalHeader, proposalPayload, *smartbftprotos.ViewMetadata, error) {
	var header proposalHeader
	if err := decodeStrictJSON(proposal.Header, &header); err != nil {
		return header, proposalPayload{}, nil, fmt.Errorf("decode proposal header: %w", err)
	}
	if header.SchemaVersion != proposalSchema && header.SchemaVersion != "mwvn-smartbft-proposal/v2" {
		return header, proposalPayload{}, nil, errors.New("invalid proposal schema_version")
	}
	var payload proposalPayload
	if err := decodeStrictJSON(proposal.Payload, &payload); err != nil {
		return header, payload, nil, fmt.Errorf("decode proposal payload: %w", err)
	}
	if (header.SchemaVersion == proposalSchema && (len(payload.ApprovedQCs) == 0 || len(payload.Commands) != 0)) ||
		(header.SchemaVersion != proposalSchema && (len(payload.Commands) != 1 || len(payload.ApprovedQCs) != 0)) {
		return header, payload, nil, errors.New("invalid versioned proposal contents")
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
	raw, err := canonicalJSON(proposalVerificationRequest{Header: header, ApprovedQCs: payload.ApprovedQCs, Commands: payload.Commands})
	if err != nil {
		return nil, err
	}
	ids, err := n.core.verifyProposal(raw)
	if err != nil {
		return nil, err
	}
	if len(ids) != len(payload.items()) {
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
	result := make([]types.RequestInfo, 0, len(payload.items()))
	for _, raw := range payload.items() {
		result = append(result, types.RequestInfo{ClientID: "mwvn-approved-qc", ID: requestID(raw)})
	}
	return result
}

func (n *bftNode) VerificationSequence() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.latest.Proposal.Header) == 0 {
		return 0
	}
	header, _, _, err := parseProposal(n.latest.Proposal)
	if err != nil {
		return 0
	}
	return header.Sequence
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
	payload := proposalPayload{}
	schema := proposalSchema
	for _, request := range requests {
		copy := append(json.RawMessage(nil), request...)
		var envelope struct {
			SchemaVersion string `json:"schema_version"`
		}
		if err := json.Unmarshal(request, &envelope); err != nil {
			n.logger.Panicf("invalid request: %v", err)
		}
		if envelope.SchemaVersion == "mwvn-command/v1" {
			schema = "mwvn-smartbft-proposal/v2"
			payload.Commands = append(payload.Commands, copy)
		} else {
			payload.ApprovedQCs = append(payload.ApprovedQCs, copy)
		}
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
	header, err := canonicalJSON(proposalHeader{SchemaVersion: schema, Sequence: viewMetadata.LatestSequence, PreviousHash: previousHash, DataHash: digestBytes(payloadBytes)})
	if err != nil {
		n.logger.Panicf("encode proposal header: %v", err)
	}
	return types.Proposal{Header: header, Payload: payloadBytes, Metadata: metadata, VerificationSequence: int64(state.Height)}
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
	ApprovedQCs    []json.RawMessage `json:"approved_qcs,omitempty"`
	Commands       []json.RawMessage `json:"commands,omitempty"`
	DecisionProof  []proofSignature  `json:"decision_proof"`
}

func (n *bftNode) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	n.deliveryMu.Lock()
	defer n.deliveryMu.Unlock()
	decision := types.Decision{Proposal: proposal, Signatures: signatures}
	if err := n.persistAndApply(decision); err != nil {
		n.logger.Panicf("durable decision delivery failed (restart to retry): %v", err)
	}
	return types.Reconfig{InLatestDecision: false}
}

func (n *bftNode) decisionCallback(proposal types.Proposal, signatures []types.Signature) ([]byte, error) {
	header, payload, metadata, err := parseProposal(proposal)
	if err != nil {
		return nil, err
	}
	for _, command := range payload.Commands {
		var identity struct {
			Network    string `json:"network_id"`
			Membership uint64 `json:"membership_version"`
		}
		if err := json.Unmarshal(command, &identity); err != nil || identity.Network != n.membership.NetworkID || identity.Membership != n.membership.Version {
			return nil, errors.New("command decision network or membership mismatch")
		}
	}
	quorum := (len(n.membership.Nodes) + (len(n.membership.Nodes)-1)/3 + 2) / 2
	if len(signatures) < quorum {
		return nil, fmt.Errorf("decision proof has %d signatures, need %d", len(signatures), quorum)
	}
	seen := make(map[uint64]struct{}, len(signatures))
	proof := make([]proofSignature, 0, len(signatures))
	for _, signature := range signatures {
		if _, duplicate := seen[signature.ID]; duplicate {
			return nil, fmt.Errorf("decision proof repeats validator %d", signature.ID)
		}
		seen[signature.ID] = struct{}{}
		if _, err := n.VerifyConsenterSig(signature, proposal); err != nil {
			return nil, fmt.Errorf("invalid decision proof: %w", err)
		}
		proof = append(proof, proofSignature{ValidatorID: signature.ID, Value: base64.StdEncoding.EncodeToString(signature.Value), Auxiliary: base64.StdEncoding.EncodeToString(signature.Msg)})
	}
	schema := "mwvn-bft-commit/v1"
	if len(payload.Commands) > 0 {
		schema = "mwvn-bft-commit/v2"
	}
	return canonicalJSON(commitRequest{SchemaVersion: schema, NodeID: n.id, Sequence: header.Sequence, View: metadata.ViewId, PreviousHash: header.PreviousHash, ProposalDigest: proposal.Digest(), ApprovedQCs: payload.ApprovedQCs, Commands: payload.Commands, DecisionProof: proof})
}

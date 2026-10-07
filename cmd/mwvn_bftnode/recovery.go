package main

// Application state transfer. These HTTP records carry existing SmartBFT
// decision proofs; they are not new consensus protocol messages.
import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type archivedDecision struct {
	Network    string         `json:"network_id"`
	Membership uint64         `json:"membership_version"`
	Decision   types.Decision `json:"decision"`
}

func (n *bftNode) restoreCheckpoint() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.latest.Proposal.Header) == 0 {
		return
	}
	metadata := &smartbftprotos.ViewMetadata{}
	_ = proto.Unmarshal(n.latest.Proposal.Metadata, metadata) // verified on load
	n.consensus.Metadata = metadata
	n.consensus.LastProposal = n.latest.Proposal
	n.consensus.LastSignatures = n.latest.Signatures
}

func (n *bftNode) archivePath(sequence uint64) string {
	return filepath.Join(n.dataDir, "decisions", fmt.Sprintf("%020d.json", sequence))
}

func (n *bftNode) loadDecision(path string) (types.Decision, error) {
	var record archivedDecision
	raw, err := os.ReadFile(path)
	if err != nil {
		return record.Decision, err
	}
	if err = decodeStrictJSON(raw, &record); err != nil {
		return record.Decision, err
	}
	if record.Network != n.membership.NetworkID || record.Membership != n.membership.Version {
		return record.Decision, fmt.Errorf("archive network or membership mismatch")
	}
	_, err = n.decisionCallback(record.Decision.Proposal, record.Decision.Signatures)
	return record.Decision, err
}

func (n *bftNode) restoreArchive(applicationHeight uint64) error {
	paths, err := filepath.Glob(filepath.Join(n.dataDir, "decisions", "*.json"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	if uint64(len(paths)) < applicationHeight {
		return fmt.Errorf("application ledger ahead of certified archive: need reviewed migration, not an automatic reset")
	}
	for index, path := range paths {
		decision, err := n.loadDecision(path)
		if err != nil {
			return fmt.Errorf("load certified archive: %w", err)
		}
		header, _, _, _ := parseProposal(decision.Proposal)
		if header.Sequence != uint64(index+1) {
			return fmt.Errorf("certified archive has a sequence gap")
		}
		// Idempotent delivery checks even the already committed application records.
		if err = n.applyDecision(decision); err != nil {
			return err
		}
	}
	n.restoreCheckpoint()
	return nil
}

func (n *bftNode) persistAndApply(decision types.Decision) error {
	if _, err := n.decisionCallback(decision.Proposal, decision.Signatures); err != nil {
		return err
	}
	header, _, _, _ := parseProposal(decision.Proposal)
	path := n.archivePath(header.Sequence)
	if existing, err := n.loadDecision(path); err == nil {
		if existing.Proposal.Digest() != decision.Proposal.Digest() {
			return fmt.Errorf("conflicting decision at sequence %d", header.Sequence)
		}
		return n.applyDecision(existing)
	} else if !os.IsNotExist(err) {
		return err
	}
	state, err := n.core.state()
	if err != nil {
		return err
	}
	if header.Sequence != state.Height+1 || header.PreviousHash != state.LedgerHead {
		return fmt.Errorf("recovery decision does not extend application ledger")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(archivedDecision{Network: n.membership.NetworkID, Membership: n.membership.Version, Decision: decision})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(raw); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		return err
	}
	return n.applyDecision(decision)
}

func (n *bftNode) applyDecision(decision types.Decision) error {
	raw, err := n.decisionCallback(decision.Proposal, decision.Signatures)
	if err != nil {
		return err
	}
	if err = n.core.commit(raw); err != nil {
		return err
	}
	header, _, _, _ := parseProposal(decision.Proposal)
	n.mu.Lock()
	current, _, _, _ := parseProposal(n.latest.Proposal)
	if header.Sequence >= current.Sequence {
		n.latest = decision
	}
	n.mu.Unlock()
	n.trace.add(traceEvent{NodeID: n.id, Direction: "commit", Sender: n.id, Receiver: n.id,
		MessageType: "LEDGER-APPEND", Sequence: &header.Sequence, ProposalDigest: decision.Proposal.Digest(), SizeBytes: len(raw), Outcome: "python_core_durable"})
	return nil
}

type archivePage struct {
	Records []archivedDecision `json:"records"`
}

func (s *nodeServer) decisions(writer http.ResponseWriter, request *http.Request) {
	raw, _, ok := s.authenticatedPeerBody(writer, request, 1024)
	if !ok {
		return
	}
	var query struct {
		From uint64 `json:"from"`
	}
	if err := decodeStrictJSON(raw, &query); err != nil || query.From == 0 {
		writeError(writer, 400, "invalid archive range")
		return
	}
	page := archivePage{Records: make([]archivedDecision, 0)}
	for i := query.From; i < query.From+16; i++ {
		decision, err := s.node.loadDecision(s.node.archivePath(i))
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			writeError(writer, 500, err.Error())
			return
		}
		page.Records = append(page.Records, archivedDecision{Network: s.node.membership.NetworkID, Membership: s.node.membership.Version, Decision: decision})
	}
	writeJSON(writer, 200, page)
}

func (n *bftNode) catchUp() error {
	n.deliveryMu.Lock()
	defer n.deliveryMu.Unlock()
	for _, member := range n.membership.Nodes {
		if member.ID == n.id {
			continue
		}
		for {
			state, err := n.core.state()
			if err != nil {
				return err
			}
			raw, _ := json.Marshal(map[string]uint64{"from": state.Height + 1})
			path := "/internal/v1/decisions"
			request, err := http.NewRequest(http.MethodPost, member.PeerURL+path, bytes.NewReader(raw))
			if err != nil {
				break
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-MWVN-Network", n.membership.NetworkID)
			request.Header.Set("X-MWVN-Sender", strconv.FormatUint(n.id, 10))
			request.Header.Set("X-MWVN-Signature", base64.StdEncoding.EncodeToString(ed25519.Sign(n.privateKey, transportSigningPayload(n.membership.NetworkID, path, raw))))
			response, err := n.comm.client.Do(request)
			if err != nil {
				break
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
			response.Body.Close()
			if err != nil || response.StatusCode != 200 {
				break
			}
			var page archivePage
			if err := decodeStrictJSON(body, &page); err != nil {
				break
			}
			if len(page.Records) == 0 {
				break
			}
			next := state.Height + 1
			valid := true
			for _, record := range page.Records {
				header, _, _, err := parseProposal(record.Decision.Proposal)
				if err != nil || header.Sequence != next || record.Network != n.membership.NetworkID || record.Membership != n.membership.Version {
					valid = false
					break
				}
				// A peer's latest-height claim is never trusted: verify every quorum
				// proof and let the application enforce chain and deterministic state.
				if err := n.persistAndApply(record.Decision); err != nil {
					n.logger.Warnf("reject state-transfer decision: %v", err)
					valid = false
					break
				}
				next++
			}
			if !valid {
				break
			}
		}
	}
	return nil
}

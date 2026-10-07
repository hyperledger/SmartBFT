package main

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func recoveryNode(t *testing.T, n int) (*bftNode, []ed25519.PrivateKey, *bool) {
	t.Helper()
	node := &bftNode{id: 1, dataDir: t.TempDir(), publicKeys: make(map[uint64]ed25519.PublicKey), trace: &traceStore{}, logger: zap.NewNop().Sugar(), consensus: &consensus.Consensus{}}
	node.membership.NetworkID = "test-network"
	node.membership.Version = 1
	keys := make([]ed25519.PrivateKey, n)
	for i := 0; i < n; i++ {
		seed := make([]byte, 32)
		seed[0] = byte(i + 1)
		keys[i] = ed25519.NewKeyFromSeed(seed)
		node.publicKeys[uint64(i+1)] = keys[i].Public().(ed25519.PublicKey)
		node.membership.Nodes = append(node.membership.Nodes, member{ID: uint64(i + 1)})
	}
	height := uint64(0)
	head := strings.Repeat("0", 64)
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			writeJSON(w, 200, coreState{NodeID: 1, Height: height, LedgerHead: head})
			return
		}
		var delivery commitRequest
		if err := json.NewDecoder(r.Body).Decode(&delivery); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if delivery.Sequence == height+1 {
			height++
			head = delivery.ProposalDigest
		}
		if fail {
			w.WriteHeader(503)
			return
		}
		writeJSON(w, 200, map[string]bool{"created": true})
	}))
	t.Cleanup(server.Close)
	node.core = newCoreClient(server.URL)
	return node, keys, &fail
}

func certifiedDecision(t *testing.T, node *bftNode, keys []ed25519.PrivateKey, sequence uint64, previous string) types.Decision {
	t.Helper()
	command, _ := canonicalJSON(map[string]any{"schema_version": "mwvn-command/v1", "request_id": "test-command", "network_id": node.membership.NetworkID, "membership_version": node.membership.Version})
	payload, _ := canonicalJSON(proposalPayload{Commands: []json.RawMessage{command}})
	header, _ := canonicalJSON(proposalHeader{SchemaVersion: "mwvn-smartbft-proposal/v2", Sequence: sequence, PreviousHash: previous, DataHash: digestBytes(payload)})
	metadata, _ := proto.Marshal(&smartbftprotos.ViewMetadata{LatestSequence: sequence})
	proposal := types.Proposal{Header: header, Payload: payload, Metadata: metadata, VerificationSequence: int64(sequence - 1)}
	decision := types.Decision{Proposal: proposal}
	for i, key := range keys {
		decision.Signatures = append(decision.Signatures, types.Signature{ID: uint64(i + 1), Value: ed25519.Sign(key, signaturePayload("smartbft-proposal/v1", proposal.Digest(), nil))})
	}
	return decision
}

func TestCertifiedArchiveRecoversAcknowledgementLoss(t *testing.T) {
	node, keys, fail := recoveryNode(t, 4)
	d := certifiedDecision(t, node, keys, 1, strings.Repeat("0", 64))
	*fail = true
	if err := node.persistAndApply(d); err == nil {
		t.Fatal("expected lost acknowledgement")
	}
	if _, err := os.Stat(node.archivePath(1)); err != nil {
		t.Fatal("decision not persisted before delivery", err)
	}
	*fail = false
	if err := node.restoreArchive(1); err != nil {
		t.Fatal(err)
	}
	if node.VerificationSequence() != 1 {
		t.Fatal("missing restored checkpoint/revalidation epoch")
	}
	if err := node.persistAndApply(d); err != nil {
		t.Fatal("retry not idempotent", err)
	}
	if node.consensus.Metadata.LatestSequence != 1 {
		t.Fatal("wrong recovered sequence")
	}
}

func TestDecisionProofQuorumAndForgery(t *testing.T) {
	for _, n := range []int{4, 5, 6, 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			node, keys, _ := recoveryNode(t, n)
			q := (n + (n-1)/3 + 2) / 2
			d := certifiedDecision(t, node, keys, 1, strings.Repeat("0", 64))
			if _, err := node.decisionCallback(d.Proposal, d.Signatures[:q]); err != nil {
				t.Fatal(err)
			}
			if _, err := node.decisionCallback(d.Proposal, d.Signatures[:q-1]); err == nil {
				t.Fatal("insufficient quorum accepted")
			}
			duplicate := append([]types.Signature(nil), d.Signatures[:q]...)
			duplicate[1] = duplicate[0]
			if _, err := node.decisionCallback(d.Proposal, duplicate); err == nil {
				t.Fatal("duplicate signer accepted")
			}
			altered := d.Proposal
			altered.Payload = []byte(`{"commands":[]}`)
			if _, err := node.decisionCallback(altered, d.Signatures); err == nil {
				t.Fatal("tampered proposal accepted")
			}
			d.Signatures[0].Value[0] ^= 1
			if _, err := node.decisionCallback(d.Proposal, d.Signatures); err == nil {
				t.Fatal("forged signature accepted")
			}
		})
	}
}

func TestRecoveryRejectsGapAndForeignNetwork(t *testing.T) {
	node, keys, _ := recoveryNode(t, 4)
	gap := certifiedDecision(t, node, keys, 2, strings.Repeat("0", 64))
	if err := node.persistAndApply(gap); err == nil {
		t.Fatal("gap accepted")
	}
	if _, err := os.Stat(node.archivePath(2)); !os.IsNotExist(err) {
		t.Fatal("gap polluted archive")
	}
	d := certifiedDecision(t, node, keys, 1, strings.Repeat("0", 64))
	node.membership.NetworkID = "other"
	if _, err := node.decisionCallback(d.Proposal, d.Signatures); err == nil {
		t.Fatal("foreign command accepted")
	}
}

func TestProposalPayloadVersionSeparation(t *testing.T) {
	node, keys, _ := recoveryNode(t, 4)
	d := certifiedDecision(t, node, keys, 1, strings.Repeat("0", 64))
	header, payload, _, err := parseProposal(d.Proposal)
	if err != nil || len(payload.items()) != 1 {
		t.Fatal(err)
	}
	header.SchemaVersion = proposalSchema
	d.Proposal.Header, _ = canonicalJSON(header)
	if _, _, _, err := parseProposal(d.Proposal); err == nil {
		t.Fatal("v1 header allowed v2 commands")
	}
}

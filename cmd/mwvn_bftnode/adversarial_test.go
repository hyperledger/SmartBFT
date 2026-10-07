package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Exercise the actual peer HTTP ingress. Rejections happen before an engine
// can consume the forged message; valid signatures do not excuse bad protobuf.
func TestPeerIngressRejectsByzantineMessages(t *testing.T) {
	for _, attack := range []string{"foreign-network", "unknown-sender", "forged-signature", "tampered-body", "wrong-path", "malformed-protobuf"} {
		t.Run(attack, func(t *testing.T) {
			node, keys, _ := recoveryNode(t, 4)
			path := "/internal/v1/consensus"
			raw := []byte{0xff}
			signature := ed25519.Sign(keys[1], transportSigningPayload(node.membership.NetworkID, path, raw))
			network, sender := node.membership.NetworkID, "2"
			expected := http.StatusUnauthorized
			switch attack {
			case "foreign-network":
				network = "foreign"
			case "unknown-sender":
				sender = "99"
			case "forged-signature":
				signature[0] ^= 1
			case "tampered-body":
				raw = append(raw, 0xff)
			case "wrong-path":
				signature = ed25519.Sign(keys[1], transportSigningPayload(network, "/internal/v1/transaction", raw))
			case "malformed-protobuf":
				expected = http.StatusBadRequest
			}
			request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
			request.Header.Set("X-MWVN-Network", network)
			request.Header.Set("X-MWVN-Sender", sender)
			request.Header.Set("X-MWVN-Signature", base64.StdEncoding.EncodeToString(signature))
			result := httptest.NewRecorder()
			newNodeServer(node).ServeHTTP(result, request)
			if result.Code != expected {
				t.Fatalf("status %d, want %d: %s", result.Code, expected, result.Body.String())
			}
			if len(node.trace.list(0, 100)) != 0 {
				t.Fatal("invalid message entered consensus trace")
			}
		})
	}
}

func TestUnknownValidatorCannotContributeDecisionProof(t *testing.T) {
	node, keys, _ := recoveryNode(t, 4)
	decision := certifiedDecision(t, node, keys, 1, "0000000000000000000000000000000000000000000000000000000000000000")
	decision.Signatures = decision.Signatures[:3]
	decision.Signatures[0].ID = 99
	if _, err := node.decisionCallback(decision.Proposal, decision.Signatures); err == nil {
		t.Fatal("unknown validator counted toward decision quorum")
	}
}

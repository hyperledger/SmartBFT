// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
)

const (
	membershipSchema = "mwvn-smartbft-membership/v1"
	proposalSchema   = "mwvn-smartbft-proposal/v1"
)

type membership struct {
	SchemaVersion string   `json:"schema_version"`
	NetworkID     string   `json:"network_id"`
	Version       uint64   `json:"version"`
	Nodes         []member `json:"nodes"`
}

type member struct {
	ID        uint64 `json:"id"`
	PeerURL   string `json:"peer_url"`
	PublicKey string `json:"public_key"`
}

func loadMembership(path string, selfID uint64) (membership, map[uint64]ed25519.PublicKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return membership{}, nil, fmt.Errorf("read membership: %w", err)
	}
	var result membership
	if err := decodeStrictJSON(raw, &result); err != nil {
		return membership{}, nil, fmt.Errorf("decode membership: %w", err)
	}
	if result.SchemaVersion != membershipSchema {
		return membership{}, nil, fmt.Errorf("schema_version must be %q", membershipSchema)
	}
	if strings.TrimSpace(result.NetworkID) == "" || result.Version == 0 {
		return membership{}, nil, errors.New("membership requires network_id and positive version")
	}
	if len(result.Nodes) < 4 {
		return membership{}, nil, fmt.Errorf("local BFT membership requires at least 4 nodes, got %d", len(result.Nodes))
	}
	keys := make(map[uint64]ed25519.PublicKey, len(result.Nodes))
	endpoints := make(map[string]struct{}, len(result.Nodes))
	foundSelf := false
	for _, node := range result.Nodes {
		if node.ID == 0 {
			return membership{}, nil, errors.New("node ID must be positive")
		}
		if _, exists := keys[node.ID]; exists {
			return membership{}, nil, fmt.Errorf("duplicate node ID %d", node.ID)
		}
		parsed, err := url.Parse(node.PeerURL)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.Path != "" {
			return membership{}, nil, fmt.Errorf("node %d peer_url must be an HTTP origin", node.ID)
		}
		if _, exists := endpoints[node.PeerURL]; exists {
			return membership{}, nil, fmt.Errorf("duplicate peer_url %q", node.PeerURL)
		}
		endpoints[node.PeerURL] = struct{}{}
		decoded, err := base64.StdEncoding.DecodeString(node.PublicKey)
		if err != nil || len(decoded) != ed25519.PublicKeySize {
			return membership{}, nil, fmt.Errorf("node %d public_key must be a base64 Ed25519 public key", node.ID)
		}
		keys[node.ID] = ed25519.PublicKey(append([]byte(nil), decoded...))
		foundSelf = foundSelf || node.ID == selfID
	}
	if !foundSelf {
		return membership{}, nil, fmt.Errorf("self node ID %d is absent from membership", selfID)
	}
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].ID < result.Nodes[j].ID })
	return result, keys, nil
}

func loadPrivateKey(path string, expected ed25519.PublicKey) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read private key seed: %w", err)
	}
	seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, errors.New("private key file must contain one base64 Ed25519 seed")
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	actual := privateKey.Public().(ed25519.PublicKey)
	if !bytes.Equal(actual, expected) {
		return nil, errors.New("private key does not match membership public key")
	}
	return privateKey, nil
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func canonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

func digestBytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type approvedQCIdentity struct {
	RequestID string `json:"request_id"`
}

func requestID(raw []byte) string {
	var identity approvedQCIdentity
	if err := json.Unmarshal(raw, &identity); err != nil || identity.RequestID == "" {
		return digestBytes(raw)
	}
	return identity.RequestID
}

type proposalHeader struct {
	SchemaVersion string `json:"schema_version"`
	Sequence      uint64 `json:"sequence"`
	PreviousHash  string `json:"previous_hash"`
	DataHash      string `json:"data_hash"`
}

type proposalPayload struct {
	ApprovedQCs []json.RawMessage `json:"approved_qcs,omitempty"`
	Commands    []json.RawMessage `json:"commands,omitempty"`
}

func (p proposalPayload) items() []json.RawMessage {
	if len(p.Commands) > 0 {
		return p.Commands
	}
	return p.ApprovedQCs
}

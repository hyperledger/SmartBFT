// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMembershipAndPrivateKey(t *testing.T) {
	directory := t.TempDir()
	members := make([]member, 0, 7)
	seeds := make(map[uint64][]byte)
	for id := uint64(1); id <= 7; id++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, member{ID: id, PeerURL: "http://node-" + string(rune('0'+id)) + ":8200", PublicKey: base64.StdEncoding.EncodeToString(public)})
		seeds[id] = private.Seed()
	}
	config := membership{SchemaVersion: membershipSchema, NetworkID: "test-network", Version: 1, Nodes: members}
	raw, _ := json.Marshal(config)
	membershipPath := filepath.Join(directory, "membership.json")
	if err := os.WriteFile(membershipPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, keys, err := loadMembership(membershipPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Nodes) != 7 || len(keys) != 7 {
		t.Fatalf("unexpected membership sizes: nodes=%d keys=%d", len(loaded.Nodes), len(keys))
	}
	keyPath := filepath.Join(directory, "node-1.seed")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(seeds[1])), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPrivateKey(keyPath, keys[1]); err != nil {
		t.Fatal(err)
	}
}

func TestTransportSigningPayloadDetectsChanges(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload := transportSigningPayload("network", "/internal/v1/consensus", []byte("message"))
	signature := ed25519.Sign(private, payload)
	if !ed25519.Verify(public, payload, signature) {
		t.Fatal("valid transport signature was rejected")
	}
	changed := transportSigningPayload("network", "/internal/v1/consensus", []byte("changed"))
	if ed25519.Verify(public, changed, signature) {
		t.Fatal("changed transport body accepted the old signature")
	}
}

func TestLoadMembershipRejectsFewerThanFourNodes(t *testing.T) {
	directory := t.TempDir()
	members := make([]member, 0, 3)
	for id := uint64(1); id <= 3; id++ {
		public, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, member{ID: id, PeerURL: "http://node-" + string(rune('0'+id)) + ":8200", PublicKey: base64.StdEncoding.EncodeToString(public)})
	}
	config := membership{SchemaVersion: membershipSchema, NetworkID: "test-network", Version: 1, Nodes: members}
	raw, _ := json.Marshal(config)
	membershipPath := filepath.Join(directory, "membership.json")
	if err := os.WriteFile(membershipPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadMembership(membershipPath, 1); err == nil {
		t.Fatal("membership with fewer than four nodes was accepted")
	}
}

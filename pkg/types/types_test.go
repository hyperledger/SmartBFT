// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types_test

import (
	"testing"

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
)

func TestRequestInfoString(t *testing.T) {
	ri := types.RequestInfo{ClientID: "alice", ID: "1"}
	assert.Equal(t, "alice:1", ri.String())
}

func TestProposalDigest(t *testing.T) {
	proposal := types.Proposal{
		Payload:              []byte{1},
		Header:               []byte{2},
		Metadata:             []byte{3},
		VerificationSequence: 4,
	}

	digest := proposal.Digest()
	assert.Len(t, digest, 64)
	assert.Equal(t, digest, proposal.Digest())

	for _, tst := range []struct {
		name   string
		mutate func(*types.Proposal)
	}{
		{name: "payload", mutate: func(p *types.Proposal) { p.Payload = []byte{5} }},
		{name: "header", mutate: func(p *types.Proposal) { p.Header = []byte{5} }},
		{name: "metadata", mutate: func(p *types.Proposal) { p.Metadata = []byte{5} }},
		{name: "verification sequence", mutate: func(p *types.Proposal) { p.VerificationSequence = 5 }},
	} {
		t.Run(tst.name, func(t *testing.T) {
			other := proposal
			tst.mutate(&other)
			assert.NotEqual(t, digest, other.Digest())
		})
	}
}

func TestCheckpoint(t *testing.T) {
	var checkpoint types.Checkpoint

	proposal, signatures := checkpoint.Get()
	assert.Equal(t, &smartbftprotos.Proposal{}, proposal)
	assert.Empty(t, signatures)

	checkpoint.Set(types.Proposal{
		Payload:              []byte{1},
		Header:               []byte{2},
		Metadata:             []byte{3},
		VerificationSequence: 4,
	}, []types.Signature{
		{ID: 1, Value: []byte{5}, Msg: []byte{6}},
		{ID: 2, Value: []byte{7}, Msg: []byte{8}},
	})

	proposal, signatures = checkpoint.Get()
	assert.Equal(t, &smartbftprotos.Proposal{
		Payload:              []byte{1},
		Header:               []byte{2},
		Metadata:             []byte{3},
		VerificationSequence: 4,
	}, proposal)
	assert.Equal(t, []*smartbftprotos.Signature{
		{Signer: 1, Value: []byte{5}, Msg: []byte{6}},
		{Signer: 2, Value: []byte{7}, Msg: []byte{8}},
	}, signatures)
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"testing"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
)

func TestNetwork(*testing.T) {
	network := NewNetwork()
	node1 := make(mockHandler)
	node2 := make(mockHandler)

	network.AddOrUpdateNode(1, node1, nil)
	network.AddOrUpdateNode(2, node2, nil)
	prepare := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_Prepare{
			Prepare: &smartbftprotos.Prepare{Seq: 1},
		},
	}
	network.StartServe()

	network.send(1, 2, prepare)
	network.send(2, 1, prepare)

	<-node1
	<-node2

	network.send(1, 2, &FwdMessage{Payload: []byte("1")})
	network.send(2, 1, &FwdMessage{Payload: []byte("1")})

	<-node1
	<-node2
}

type mockHandler chan msgFrom

func (mh mockHandler) HandleMessage(sender uint64, m *smartbftprotos.Message) {
	mh <- msgFrom{from: int(sender), message: m}
}

func (mh mockHandler) HandleRequest(sender uint64, req []byte) {
	mh <- msgFrom{from: int(sender), message: &FwdMessage{Payload: req}}
}

func (mh mockHandler) Stop() {}

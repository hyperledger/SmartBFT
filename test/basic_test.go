// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

const (
	realViewChangeTimeout = 90 * time.Second
)

func TestBasic(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	for i := 1; i < 5; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestNodeViewChangeWhileInPartition(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	var once sync.Once
	var viewChangeTimeoutWG sync.WaitGroup
	viewChangeTimeoutWG.Add(1)

	var viewChangeWG sync.WaitGroup
	viewChangeWG.Add(1)

	var syncWG sync.WaitGroup
	syncWG.Add(1)

	baseLogger := nodes[3].logger.Desugar()
	nodes[3].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "is calling sync because it got a view change timeout") {
			once.Do(func() {
				viewChangeTimeoutWG.Done()
			})
		}

		if strings.Contains(entry.Message, "Node 4 is setting the checkpoint after sync returned with view 0 and seq 1") {
			syncWG.Done()
		}

		if strings.Contains(entry.Message, "ViewChanged, the new view is 1") {
			viewChangeWG.Done()
		}

		return nil
	})).Sugar()

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	startNodes(nodes, network)

	// Ensure the last node is disconnected and control its Sync()
	nodes[len(nodes)-1].Disconnect()

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to a node

	for i := range numberOfNodes - 1 {
		<-nodes[i].Delivered
	}

	// Disconnect leader to force view change on everyone
	nodes[0].Disconnect()

	// Accelerate the time until a view change because of heartbeat timeout
	done := make(chan struct{})
	defer func() {
		close(done)
	}()

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	// Wait for view change timeout to occur
	viewChangeTimeoutWG.Wait()
	// Connect the last node to let it participate in view change
	nodes[len(nodes)-1].Connect()
	// Ensure the last node calls sync
	syncWG.Wait()
	// Ensure the last node successfully delivers the block to the application
	<-nodes[len(nodes)-1].Delivered
	// Make sure view change occurred
	viewChangeWG.Wait()
}

func TestRestartFollowers(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	startViewWG := sync.WaitGroup{}
	startViewWG.Add(1)
	baseLogger := nodes[2].logger.Desugar()
	nodes[2].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Starting view with number 0, sequence 2") {
			startViewWG.Done()
		}
		return nil
	})).Sugar()
	nodes[2].Setup()

	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	d0 := <-nodes[0].Delivered
	d2 := <-nodes[2].Delivered
	d3 := <-nodes[3].Delivered

	nodes[1].Restart()
	d1 := <-nodes[1].Delivered

	data = append(data, d0, d1, d2, d3)
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[2].Restart()
	startViewWG.Wait()

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	d0 = <-nodes[0].Delivered
	d1 = <-nodes[1].Delivered
	d2 = <-nodes[2].Delivered

	nodes[3].Restart()
	d3 = <-nodes[3].Delivered

	data = append(data, d0, d1, d2, d3)
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderInPartition(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	assert.Equal(t, uint64(0), nodes[2].Consensus.GetLeaderID())
	startNodes(nodes, network)
	assert.Equal(t, uint64(1), nodes[2].Consensus.GetLeaderID())

	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}
	assert.LessOrEqual(t, uint64(2), nodes[2].Consensus.GetLeaderID())
}

func TestAfterDecisionLeaderInPartition(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "3", ClientID: "alice"}) // submit to other nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "4", ClientID: "alice"}) // submit to other nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderInPartitionWithHealing(t *testing.T) {
	t.Parallel()

	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition
	t.Log("Disconnected n0")

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "3", ClientID: "alice"}) // submit to other nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}

	assert.Len(t, nodes[0].Delivered, 0) // n0 did not receive it

	nodes[0].Connect() // partition heals, leader should eventually sync, become a follower, and deliver
	t.Log("Connected n0")

	data0 := <-nodes[0].Delivered
	assert.Equal(t, data[0], data0)
}

func TestMultiLeadersPartition(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}
	assert.Equal(t, uint64(0), nodes[0].Consensus.GetLeaderID())
	startNodes(nodes, network)
	assert.Equal(t, uint64(1), nodes[0].Consensus.GetLeaderID())

	nodes[0].Disconnect() // leader in partition
	nodes[1].Disconnect() // next leader in partition

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	done := make(chan struct{})
	defer close(done)

	var counter uint64
	accelerateTime(nodes, done, false, true, &counter)

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 3 {
		assert.Equal(t, data[i], data[i+1])
	}

	lID := nodes[2].Consensus.GetLeaderID()
	assert.LessOrEqual(t, uint64(3), lID)
	for i := 3; i < numberOfNodes; i++ {
		assert.Equal(t, lID, nodes[i].Consensus.GetLeaderID())
	}
}

func TestHeartbeatTimeoutCausesViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.Setup()
	}

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(numberOfNodes - 1)
	for _, n := range nodes {
		baseLogger := n.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		n.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "ViewChanged") {
				viewChangeWG.Done()
			}
			return nil
		})).Sugar()
	}

	startNodes(nodes, network)

	nodes[0].Disconnect() // leader in partition

	// Accelerate the time until a view change because of heartbeat timeout
	var counter uint64
	accelerateTime(nodes, done, true, false, &counter)

	viewChangeWG.Wait()
	close(done)

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestMultiViewChangeWithNoRequestsTimeout(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(5)

	ns := network.Nodes()
	for _, n := range ns {
		baseLogger := n.app.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		n.app.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "ViewChanged") {
				viewChangeWG.Done()
			}
			return nil
		})).Sugar()
	}

	startNodes(nodes, network)

	nodes[0].Disconnect() // leader in partition
	nodes[1].Disconnect() // next leader in partition

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)
	viewChangeWG.Wait()
	close(done)

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 3 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestCatchingUpWithViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	done := make(chan struct{})
	viewChangeFinishWG := sync.WaitGroup{}
	viewChangeFinishWG.Add(1)
	viewChangeFinishOnce := sync.Once{}
	baseLogger := nodes[3].logger.Desugar()
	nodes[3].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeFinishOnce.Do(func() {
				viewChangeFinishWG.Done()
			})
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, network)

	nodes[3].Disconnect() // will need to catch up

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes - 1 {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[3].Connect()
	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "2", ClientID: "alice"}) // submit to other nodes
	}

	var counter uint64
	accelerateTime(nodes, done, false, true, &counter)

	data3 := <-nodes[3].Delivered // from catch up
	assert.Equal(t, data[0], data3)

	viewChangeFinishWG.Wait()
	close(done)

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderCatchingUpAfterViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "2", ClientID: "alice"}) // submit to other nodes
	}
	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Connect() // old leader woke up

	// We create new batches until it catches up
	for reqID := 3; reqID < 100; reqID++ {
		nodes[1].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		nodes[2].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		<-nodes[1].Delivered // Wait for new leader to commit
		<-nodes[2].Delivered // Wait for follower to commit
		caughtUp := waitForCatchup(reqID, nodes[0].Delivered)
		if caughtUp {
			return
		}
	}
	t.Fatalf("Didn't catch up")
}

func TestRestartAfterViewChangeAndRestoreNewView(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.Setup()
	}

	// wait for a view change to occur
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(2)
	baseLogger1 := nodes[1].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[1].Consensus.Logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()
	baseLogger3 := nodes[3].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[3].Consensus.Logger = baseLogger3.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()

	startNodes(nodes, network)

	nodes[0].Disconnect()

	var counter uint64
	accelerateTime(nodes, done, true, false, &counter)

	viewChangeWG.Wait()
	close(done)

	// restart new leader and a follower, they will restore from new view
	nodes[1].Restart()
	nodes[3].Restart()

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}
	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestRestoringViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	done := make(chan struct{})
	viewChangeFinishWG := sync.WaitGroup{}
	viewChangeFinishWG.Add(1)
	viewChangeFinishOnce := sync.Once{}
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(1)
	viewChangeOnce := sync.Once{}
	baseLogger := nodes[6].logger.Desugar()
	nodes[6].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Node 7 sent view data msg") {
			viewChangeOnce.Do(func() {
				viewChangeWG.Done()
			})
		}
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeFinishOnce.Do(func() {
				viewChangeFinishWG.Done()
			})
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, network)

	nodes[0].Disconnect() // leader in partition
	nodes[1].Disconnect() // next leader in partition

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	viewChangeWG.Wait()
	nodes[6].Disconnect()
	nodes[6].Restart()
	nodes[6].Connect()

	viewChangeFinishWG.Wait()
	close(done)

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 3 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderForwarding(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[1].Submit(Request{ID: "1", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "2", ClientID: "bob"})
	nodes[3].Submit(Request{ID: "3", ClientID: "carol"})

	numBatchesCreated := countCommittedBatches(nodes[0])

	committedBatches := make([][]AppRecord, 3)
	for nodeIndex, n := range []*App{nodes[1], nodes[2], nodes[3]} {
		committedBatches = append(committedBatches, make([]AppRecord, numBatchesCreated))
		for range numBatchesCreated {
			record := <-n.Delivered
			committedBatches[nodeIndex] = append(committedBatches[nodeIndex], *record)
		}
	}

	assert.Equal(t, committedBatches[0], committedBatches[1])
	assert.Equal(t, committedBatches[0], committedBatches[2])
}

func TestLeaderExclusion(t *testing.T) {
	// Scenario: The leader doesn't send messages to n3,
	// but it should detect this and sync.
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	startNodes(nodes, network)

	nodes[0].DisconnectFrom(3)

	// We create new batches until the disconnected node catches up the quorum.
	for reqID := 1; reqID < 100; reqID++ {
		nodes[1].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		<-nodes[1].Delivered // Wait for follower to commit
		caughtUp := waitForCatchup(reqID, nodes[3].Delivered)
		if caughtUp {
			return
		}
	}
	t.Fatalf("Didn't catch up")
}

func TestCatchingUpWithSyncAssisted(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[3].Disconnect() // will need to catch up

	for i := 1; i <= 10; i++ {
		for j := range 3 {
			nodes[j].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
		}
		for j := range 3 {
			<-nodes[j].Delivered
		}
	}

	nodes[3].Connect()

	// We create new batches until it catches up the quorum.
	for reqID := 11; reqID < 100; reqID++ {
		for j := range 3 {
			nodes[j].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		}
		for j := range 3 {
			<-nodes[j].Delivered
		}
		caughtUp := waitForCatchup(reqID, nodes[3].Delivered)
		if caughtUp {
			return
		}
	}
	t.Fatalf("Didn't catch up")
}

func TestCatchingUpWithSyncAutonomous(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 3)
		nodes = append(nodes, n)
	}

	var detectedSequenceGap uint32

	baseLogger := nodes[3].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[3].Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Leader's sequence is 10 and ours is 1") {
			atomic.StoreUint32(&detectedSequenceGap, 1)
		}
		return nil
	})).Sugar()

	start := time.Now()
	nodes[0].heartbeatTime = make(chan time.Time, 1)
	nodes[0].heartbeatTime <- start
	nodes[3].heartbeatTime = make(chan time.Time, 1)
	nodes[3].heartbeatTime <- start
	nodes[3].viewChangeTime = make(chan time.Time, 1)
	nodes[3].viewChangeTime <- start
	nodes[0].Setup()
	nodes[3].Setup()

	startNodes(nodes, network)

	nodes[3].Disconnect() // will need to catch up

	for i := 1; i <= 5; i++ {
		for j := range 3 {
			nodes[j].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
		}
		for j := range 3 {
			<-nodes[j].Delivered
		}
	}

	nodes[3].Connect()

	done := make(chan struct{})
	// Accelerate the time for n3 so it will suspect the leader and view change.
	go func() {
		var i int
		for {
			i++
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 100):
				nodes[0].heartbeatTime <- time.Now().Add(time.Second * time.Duration(10*i))
				nodes[3].heartbeatTime <- time.Now().Add(time.Second * time.Duration(10*i))
				nodes[3].viewChangeTime <- time.Now().Add(time.Minute * time.Duration(10*i))
			}
		}
	}()

	for i := 1; i <= 5; i++ {
		select {
		case <-nodes[3].Delivered:
		case <-time.After(time.Second * 10):
			t.Fatalf("Didn't catch up within a timely period")
		}
	}

	close(done)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&detectedSequenceGap))
}

func TestSyncSameHeightPreservesDecisionsInView(t *testing.T) {
	// Scenario:
	// we inject a fake heartbeat with a higher view number to node 4 (index 3).
	// The HeartbeatMonitor sees hb.View > hm.view and calls handler.Sync()
	// directly — no view change involved. Since node 4 already has all blocks,
	// sync() returns the same height.
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	// Hook node 4's logger to detect:
	// 1. When sync is processed (so we know it's safe to submit more proposals)
	// 2. The bug symptom: "Expected decisions in view" validation failure
	var syncProcessed uint32
	bugDetected := make(chan struct{}, 1)
	baseLogger := nodes[3].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[3].Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "get msg from syncChan") {
			atomic.StoreUint32(&syncProcessed, 1)
		}
		if strings.Contains(entry.Message, "Expected decisions in view") {
			select {
			case bugDetected <- struct{}{}:
			default:
			}
		}
		return nil
	})).Sugar()

	startNodes(nodes, network)

	// Phase 1: Submit 5 proposals to build up DecisionsInView on all nodes.
	for i := 1; i <= 5; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
		for j := range numberOfNodes {
			<-nodes[j].Delivered
		}
	}

	// Phase 2:
	// the bug this test validate depended on non-deterministic Go select ordering
	// in the [internal.bft.Controller] run() function:
	//  select {
	//  case newView := <-c.viewChange:
	//  case <-c.syncChan:
	//  }
	// That's why it can't be reliably reproduced through the natural path
	// in tests — hence the fake heartbeat that triggers only sync, guaranteeing
	// the bug path.
	//
	// The test injects a fake heartbeat with View=1 (higher than current view 0)
	// to node 4. The HeartbeatMonitor sees hb.View > hm.view and calls
	// handler.Sync() directly, bypassing any view change.
	// The sender must be the leader (ID 1) to pass the leaderID check.
	fakeHeartbeat := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: 1, // higher than node 4's current view (0)
				Seq:  6,
			},
		},
	}
	nodes[3].Consensus.HandleMessage(1, fakeHeartbeat)

	// Wait for the sync to be processed by the controller's run loop.
	assert.Eventually(t, func() bool {
		return atomic.LoadUint32(&syncProcessed) == 1
	}, 30*time.Second, 50*time.Millisecond,
		"Node 4 should process sync triggered by fake heartbeat")

	// Phase 3: Submit more proposals. Without the bug fix, node 4's view was
	// restarted with DecisionsInView=0, so it rejects proposals from the
	// leader (which has DecisionsInView=5).
	for i := 6; i <= 10; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
		for j := range numberOfNodes {
			select {
			case <-nodes[j].Delivered:
			case <-bugDetected:
				t.Fatalf("DecisionsInView validation failed on node: sync at same height reset DecisionsInView to 0")
			case <-time.After(30 * time.Second):
				t.Fatalf("Node %d did not deliver proposal %d within timeout", j+1, i)
			}
		}
	}
}

func TestFollowerStateTransfer(t *testing.T) {
	// Scenario: the leader (n0) is disconnected and so there is a view change
	// a follower (n6) is also disconnected and misses the view change
	// after the follower reconnects and gets a view change timeout is calls sync
	// where it collects state transfer requests and sees that there was a view change

	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	syncedWG := sync.WaitGroup{}
	syncedWG.Add(1)
	baseLogger6 := nodes[6].logger.Desugar()
	nodes[6].logger = baseLogger6.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "collected state") {
			syncedWG.Done()
		}
		return nil
	})).Sugar()

	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(1)
	baseLogger1 := nodes[1].logger.Desugar()
	nodes[1].logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, network)

	nodes[0].Disconnect() // leader in partition
	nodes[6].Disconnect() // follower in partition

	// Accelerate the time until a view change
	done := make(chan struct{})

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	viewChangeWG.Wait()
	nodes[6].Connect()
	syncedWG.Wait()
	close(done)

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 3 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderModifiesPreprepare(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		description  string
		mutatingFunc func(target uint64, m *smartbftprotos.Message)
	}{
		{
			description: "wrong view",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().View = 1
			},
		},
		{
			description: "conflicting proposals",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				incReq := Request{ID: fmt.Sprintf("%d", target), ClientID: "alice"}
				incData := batch{
					Requests: [][]byte{incReq.ToBytes()},
				}
				m.GetPrePrepare().Proposal.Payload = incData.toBytes()
			},
		},
		{
			description: "next sequence",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().Seq = 2
			},
		},
		{
			description: "wrong verification sequence",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().Proposal.VerificationSequence = 3
			},
		},
		{
			description: "wrong number of decisions",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().Proposal.Metadata = bft.MarshalOrPanic(&smartbftprotos.ViewMetadata{
					DecisionsInView: 1, // instead of 0
					LatestSequence:  1,
					ViewId:          0,
				})
			},
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()
			network := NewNetwork()
			defer network.Shutdown()

			testDir, err := os.MkdirTemp("", test.description)
			assert.NoErrorf(t, err, "generate temporary test dir")
			defer os.RemoveAll(testDir)

			numberOfNodes := 4
			nodes := make([]*App, 0)
			for i := 1; i <= numberOfNodes; i++ {
				n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
				nodes = append(nodes, n)
			}

			once := sync.Once{}
			viewChangeWG := sync.WaitGroup{}
			viewChangeWG.Add(1)
			baseLogger1 := nodes[1].logger.Desugar()
			nodes[1].logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, "ViewChanged") {
					once.Do(func() {
						viewChangeWG.Done()
					})
				}
				return nil
			})).Sugar()
			nodes[1].Setup()

			startNodes(nodes, network)

			for i := 2; i <= numberOfNodes; i++ {
				nodes[0].MutateSend(uint64(i), test.mutatingFunc)
			}

			for i := range numberOfNodes {
				nodes[i].Submit(Request{ID: fmt.Sprintf("%d", 1), ClientID: "alice"})
			}

			viewChangeWG.Wait()

			data := make([]*AppRecord, 0)
			for i := range numberOfNodes {
				d := <-nodes[i].Delivered
				data = append(data, d)
			}
			for i := range numberOfNodes - 1 {
				assert.Equal(t, data[i], data[i+1])
			}

			md := &smartbftprotos.ViewMetadata{}
			if err := proto.Unmarshal(data[0].Metadata, md); err != nil {
				assert.NoError(t, err)
			}
			assert.Equal(t, uint64(1), md.LatestSequence)

			for i := 2; i <= numberOfNodes; i++ {
				nodes[0].ClearMutateSend(uint64(i))
			}
		})
	}
}

func TestLeaderCatchUpWithoutSync(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	restartWG := sync.WaitGroup{}
	restartWG.Add(1)

	restoredWG := sync.WaitGroup{}
	restoredWG.Add(1)

	var blockCommits uint32
	nodes[0].LoseMessages(func(msg *smartbftprotos.Message) bool {
		return msg.GetCommit() != nil && atomic.LoadUint32(&blockCommits) == 1
	})
	baseLogger := nodes[0].logger.Desugar()
	nodes[0].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Processed prepares for proposal with seq 1") {
			restartWG.Done()
		}
		if strings.Contains(entry.Message, "Restored proposal with sequence 1") {
			restoredWG.Done()
		}
		return nil
	})).Sugar()
	nodes[0].Setup()
	atomic.StoreUint32(&blockCommits, 1)

	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	restartWG.Wait()
	nodes[0].RestartSync(false)
	atomic.StoreUint32(&blockCommits, 0)
	restoredWG.Wait()

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderProposeAfterRestartWithoutSync(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	restartWG := sync.WaitGroup{}
	restartWG.Add(1)

	restoredWG := sync.WaitGroup{}
	restoredWG.Add(1)

	contViewWG := sync.WaitGroup{}
	contViewWG.Add(2)

	var blockCommits uint32
	nodes[0].LoseMessages(func(msg *smartbftprotos.Message) bool {
		return msg.GetCommit() != nil && atomic.LoadUint32(&blockCommits) == 1
	})
	baseLogger := nodes[0].logger.Desugar()
	nodes[0].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Processed prepares for proposal with seq 1") {
			restartWG.Done()
		}
		if strings.Contains(entry.Message, "Restored proposal with sequence 1") {
			restoredWG.Done()
		}
		if strings.Contains(entry.Message, "processed commits for proposal with seq 1") {
			contViewWG.Wait()
		}
		if strings.Contains(entry.Message, "GetMetadata with view 0, seq 1") {
			contViewWG.Done()
		}
		if strings.Contains(entry.Message, "Not acquiring leader token when starting view with phase PREPARED") {
			contViewWG.Done()
		}
		if strings.Contains(entry.Message, "Expected proposal sequence 2 but got 1") {
			panic("Expected proposal sequence 2 but got 1")
		}
		return nil
	})).Sugar()
	nodes[0].Setup()
	atomic.StoreUint32(&blockCommits, 1)

	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	restartWG.Wait()
	nodes[0].RestartSync(false)
	atomic.StoreUint32(&blockCommits, 0)
	restoredWG.Wait()

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestGradualStart(t *testing.T) {
	// Scenario: initially the network has only one node
	// a transaction is submitted and committed with that node
	// then another node is introduced, with a reset to the original node
	// and another transaction is passed
	// lastly a third node is added and another transaction is submitted
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	// start with only one node
	n0 := newNode(uint64(1), network, t.Name(), testDir, true, 1)

	if err := n0.Consensus.Start(); err != nil {
		n0.logger.Panicf("Consensus returned an error : %v", err)
	}
	network.StartServe()

	n0.Submit(Request{ID: fmt.Sprintf("%d", 1), ClientID: "alice"})

	d0 := <-n0.Delivered
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d0.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(0), md.ViewId)
	assert.Equal(t, uint64(1), md.LatestSequence)
	first := d0

	network.StopServe()

	// add a second node
	n1 := newNode(uint64(2), network, t.Name(), testDir, true, 1)
	atomic.AddUint64(&n1.verificationSeq, 1)

	if err := n1.Consensus.Start(); err != nil {
		n1.logger.Panicf("Consensus returned an error : %v", err)
	}

	atomic.AddUint64(&n0.verificationSeq, 1)
	n0.Restart()

	network.StartServe()

	d1 := <-n1.Delivered
	assert.Equal(t, d0, d1)

	n0.Submit(Request{ID: fmt.Sprintf("%d", 2), ClientID: "alice"})

	d0 = <-n0.Delivered
	md = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d0.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(0), md.ViewId)
	assert.Equal(t, uint64(2), md.LatestSequence)
	second := d0

	d1 = <-n1.Delivered
	assert.Equal(t, d0, d1)

	network.StopServe()

	// add a third node
	n2 := newNode(uint64(3), network, t.Name(), testDir, true, 1)
	atomic.AddUint64(&n2.verificationSeq, 2)

	if err := n2.Consensus.Start(); err != nil {
		n2.logger.Panicf("Consensus returned an error : %v", err)
	}

	atomic.AddUint64(&n1.verificationSeq, 1)
	atomic.AddUint64(&n0.verificationSeq, 1)
	n0.Restart()
	n1.Restart()

	network.StartServe()

	d2 := <-n2.Delivered
	assert.Equal(t, first, d2)
	d2 = <-n2.Delivered
	assert.Equal(t, second, d2)

	n0.Submit(Request{ID: fmt.Sprintf("%d", 3), ClientID: "alice"})

	d0 = <-n0.Delivered
	md = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d0.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(0), md.ViewId)
	assert.Equal(t, uint64(3), md.LatestSequence)

	d1 = <-n1.Delivered
	assert.Equal(t, d0, d1)
	d2 = <-n2.Delivered
	assert.Equal(t, d0, d2)

	n0.Disconnect() // disconnect the leader

	n1.Submit(Request{ID: fmt.Sprintf("%d", 4), ClientID: "alice"})
	n2.Submit(Request{ID: fmt.Sprintf("%d", 4), ClientID: "alice"})

	d1 = <-n1.Delivered
	md = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d1.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(1), md.ViewId) // view was changed
	assert.Equal(t, uint64(4), md.LatestSequence)

	d2 = <-n2.Delivered
	assert.Equal(t, d1, d2)
}

func TestReconfigAndViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 2; i <= numberOfNodes+1; i++ { // add 4 nodes with ids starting at 2
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			return false
		})
		nodes = append(nodes, n)
	}

	startNodes(nodes, network)

	for i := 1; i < 5; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := range numberOfNodes {
		nodes[i].Consensus.Stop() // stop all nodes
	}

	newNode := newNode(1, network, t.Name(), testDir, false, 0) // add node with id 1, should be the leader
	newNode.LoseMessages(func(msg *smartbftprotos.Message) bool {
		return false
	})
	nodes = append(nodes, newNode)
	startNodes(nodes[4:], network)

	for i := range numberOfNodes {
		nodes[i].Restart() // restart all stopped nodes
	}

	d := <-nodes[4].Delivered
	assert.Equal(t, d, data[0]) // make sure the new added node (leader) is synced

	nodes[4].Disconnect() // leader in partition

	for i := range numberOfNodes {
		nodes[i].Submit(Request{ID: "10", ClientID: "alice"}) // submit while leader in partition will cause a view change
	}

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestRotateAndViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	var once sync.Once
	syncedWG := sync.WaitGroup{}
	syncedWG.Add(1)
	viewChangeWG3 := sync.WaitGroup{}
	viewChangeWG3.Add(1)
	baseLogger3 := nodes[3].logger.Desugar()
	nodes[3].logger = baseLogger3.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Node 4 collected state with view 1 and sequence 4") {
			syncedWG.Done()
		}
		if strings.Contains(entry.Message, "Starting view with number 1") {
			once.Do(func() {
				viewChangeWG3.Done()
			})
		}
		return nil
	})).Sugar()

	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(1)
	baseLogger1 := nodes[1].logger.Desugar()
	nodes[1].logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to first leader

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[1].Submit(Request{ID: "2", ClientID: "alice"}) // submit to second leader

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[2].Submit(Request{ID: "3", ClientID: "alice"}) // submit to third leader

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[3].Disconnect() // fourth leader in partition

	// Accelerate the time until a view change
	done := make(chan struct{})
	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	viewChangeWG.Wait()
	nodes[3].Connect()
	syncedWG.Wait()
	viewChangeWG3.Wait()
	close(done)

	nodes[1].Submit(Request{ID: "4", ClientID: "alice"}) // submit to current leader

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestMigrateToBlacklistAndBackAgain(t *testing.T) { //nolint:tparallel // subtests are sequential phases sharing the same nodes
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	var nodes []*App
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	var leaderRotationDisabled uint64
	var boundCommitSignatures uint64

	setupLogger := func(node *App) {
		node.logger = node.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Bound 3 commit signatures to proposal") {
				atomic.AddUint64(&boundCommitSignatures, 1)
			}
			if strings.Contains(entry.Message, "Leader rotation is disabled, will not bind signatures to proposals") {
				atomic.AddUint64(&leaderRotationDisabled, 1)
			}
			return nil
		})).Sugar()
	}

	for _, n := range nodes {
		setupLogger(n)
		n.Setup()
	}

	t.Run("No leader rotation", func(t *testing.T) {
		startNodes(nodes, network)

		nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

		for i := range numberOfNodes {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(1), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(0), atomic.LoadUint64(&boundCommitSignatures))

		nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

		for i := range numberOfNodes {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(0), atomic.LoadUint64(&boundCommitSignatures))
	})

	t.Run("Activate leader rotation", func(t *testing.T) {
		network.StopServe()

		for _, n := range nodes {
			n.Consensus.Config.DecisionsPerLeader = 1
			n.Consensus.Config.LeaderRotation = true
			n.Restart()
		}

		network.StartServe()

		nodes[0].Submit(Request{ID: "3", ClientID: "alice"})

		for i := range numberOfNodes {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(1), atomic.LoadUint64(&boundCommitSignatures))

		nodes[0].Submit(Request{ID: "4", ClientID: "alice"})

		for i := range numberOfNodes {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(2), atomic.LoadUint64(&boundCommitSignatures))
	})

	t.Run("Deactivate leader rotation", func(t *testing.T) {
		t.Skip()
		network.StopServe()

		for _, n := range nodes {
			n.Consensus.Config.DecisionsPerLeader = 0
			n.Consensus.Config.LeaderRotation = false
			n.Restart()
		}

		network.StartServe()

		nodes[0].Submit(Request{ID: "5", ClientID: "alice"})

		for i := range numberOfNodes {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&boundCommitSignatures))

		nodes[0].Submit(Request{ID: "6", ClientID: "alice"})

		for i := range numberOfNodes {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&boundCommitSignatures))
	})
}

func TestNodeInFlightFails(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)

	var failedViewChange sync.WaitGroup
	failedViewChange.Add(1)

	var timeoutExpiredWG sync.WaitGroup
	timeoutExpiredWG.Add(1)

	var collectPreparesWG sync.WaitGroup
	collectPreparesWG.Add(4)

	var inFlightCommit sync.WaitGroup
	inFlightCommit.Add(2)

	var blockCommits uint32
	var blockCommitsForLastNode uint32

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false                       // Prevent last node from syncing without a reason
		n.Consensus.Config.ViewChangeTimeout = time.Second * 10      // Make view change faster to speedup the test
		n.Consensus.Config.LeaderHeartbeatTimeout = time.Second * 20 // Force view change sooner to speedup test
		n.Consensus.Config.RequestForwardTimeout = time.Second * 25  // Prevent first node to forward after view change
		n.Consensus.Config.RequestComplainTimeout = time.Second * 30 // Must be bigger than the forward timeout
		n.Consensus.Config.SpeedUpViewChange = false
		nodes = append(nodes, n)

		baseLogger := n.logger.Desugar()

		id := n.ID
		n.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "collected 2 prepares") && atomic.LoadUint32(&blockCommits) == 1 {
				collectPreparesWG.Done()
			}

			if strings.Contains(entry.Message, "In-flight view 0 with latest sequence 1 has committed a decision") && (id == 2 || id == 3) {
				inFlightCommit.Done()
			}

			if strings.Contains(entry.Message, "Timeout expired waiting on In-flight 0 with latest sequence view to commit 1") && id == 4 {
				timeoutExpiredWG.Done()
			}

			if strings.Contains(entry.Message, "Node 4 was unable to commit the in flight proposal, not changing the view") && id == 4 {
				failedViewChange.Done()
			}

			return nil
		})).Sugar()

		// Make all nodes but the last node lose commits per the 'blockCommits' flag
		if i == 4 {
			continue
		}
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			if msg.GetCommit() == nil {
				return false
			}
			return atomic.LoadUint32(&blockCommits) == 1
		})
	}

	// However the last node never receives any commits.
	nodes[len(nodes)-1].LoseMessages(func(msg *smartbftprotos.Message) bool {
		return msg.GetCommit() != nil && atomic.LoadUint32(&blockCommitsForLastNode) == 1
	})

	startNodes(nodes, network)

	atomic.StoreUint32(&blockCommits, 1)
	atomic.StoreUint32(&blockCommitsForLastNode, 1)

	nodes[0].Submit(Request{ID: "first request", ClientID: "alice"})

	// Wait for prepares to be collected
	collectPreparesWG.Wait()

	// Ensure no one commits a block because of not enough commits gathered
	var doNotCommitWG sync.WaitGroup
	doNotCommitWG.Add(numberOfNodes)

	for i := range numberOfNodes {
		go func(deliverChannel chan *AppRecord) {
			defer doNotCommitWG.Done()
			select {
			case <-deliverChannel:
				assert.Fail(t, "should not have committed because first commit message should have been lost")
			case <-time.After(time.Second):
			}
		}(nodes[i].Delivered)
	}
	doNotCommitWG.Wait()

	// Re-enable commit messages to flow to all nodes but the last node
	atomic.StoreUint32(&blockCommits, 0)

	// Disconnect leader to force view change
	nodes[0].Disconnect()

	// Wait for view change and ensure in-flight view has committed the previous decision
	inFlightCommit.Wait()

	// Re-connect the leader
	nodes[0].Connect()

	// Ensure that only nodes 1 to 3 have committed in-flight
	d1 := <-nodes[0].Delivered
	d2 := <-nodes[1].Delivered
	d3 := <-nodes[2].Delivered
	select {
	case <-nodes[3].Delivered:
		t.Fatalf("Node 4 has committed but shouldn't have")
	case <-time.After(time.Second):
	}

	// Ensure all nodes that committed, committed the same decision
	assert.Equal(t, d1, d2)
	assert.Equal(t, d2, d3)

	// Wait for in-flight view of view changer to time out
	timeoutExpiredWG.Wait()
	// Re-enable commits to flow to the last node
	atomic.StoreUint32(&blockCommitsForLastNode, 0)
	// Ensure the last node failed changing the view
	failedViewChange.Wait()
	// However, it eventually syncs successfully and delivers the proposal
	select {
	case d4 := <-nodes[3].Delivered:
		assert.Equal(t, d3, d4)
	case <-time.After(time.Second * 30):
		t.Fatalf("Didn't commit decision in a timely manner")
	}
}

func TestBlacklistAndRedemption(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 10
	var nodes []*App
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
		nodes = append(nodes, n)
	}

	var viewChangedWG sync.WaitGroup
	viewChangedWG.Add(len(nodes) - 1)

	var blacklistPrunedWG sync.WaitGroup
	blacklistPrunedWG.Add(len(nodes) + 1) // Once at proposing, n times at verification

	var redemptionWG sync.WaitGroup
	redemptionWG.Add(len(nodes))

	start := time.Now()
	for _, n := range nodes {
		l := n.logger.Desugar()
		n.logger = l.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number 1, sequence 1,") {
				viewChangedWG.Done()
			}
			if strings.Contains(entry.Message, "Blacklist changed: [1] --> []") {
				blacklistPrunedWG.Done()
			}
			if strings.Contains(entry.Message, "Rotating leader from 10 to 1") {
				redemptionWG.Done()
			}
			return nil
		})).Sugar()
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	startNodes(nodes, network)

	nodes[0].Disconnect() // Leader is in partition

	// Accelerate the time until a view change
	done := make(chan struct{})
	var counter uint64
	accelerateTime(nodes[1:], done, true, true, &counter)

	// Wait for view change
	viewChangedWG.Wait()
	close(done)

	// Rotate the leader and ensure the view doesn't change, which proves
	// node 1 never became the leader again
	for j := range numberOfNodes {
		nodes[1].Submit(Request{ID: fmt.Sprintf("%d", j), ClientID: "alice"})
		for i := 1; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}
	}

	// Ensure we remain on view 1, and that node 1 is in the blacklist
	md := &smartbftprotos.ViewMetadata{}
	err = proto.Unmarshal(nodes[1].lastDecision.Proposal.Metadata, md)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), md.ViewId)
	assert.Equal(t, []uint64{1}, md.BlackList)

	// Next, re-connect node 1
	nodes[0].Connect()

	// Accelerate time to wait for it to synchronize
	done = make(chan struct{})
	accelerateTime(nodes, done, true, true, &counter)
	for range numberOfNodes {
		<-nodes[0].Delivered
	}
	close(done)

	// Rotate the leader and ensure the view doesn't change,
	// but this time we want to ensure that node 1 became the leader
	// again at some point, and that it has been deleted from the blacklist.
	stop := make(chan struct{})
	f := func() {
		txID := make([]byte, 15)
		rand.Read(txID)
		nodes[1].Submit(Request{ID: hex.EncodeToString(txID), ClientID: "alice"})
		for i := range nodes {
			select {
			case <-nodes[i].Delivered:
			case <-stop:
				return
			}
		}
		md := &smartbftprotos.ViewMetadata{}
		err = proto.Unmarshal(nodes[1].lastDecision.Proposal.Metadata, md)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), md.ViewId)
	}
	go doInBackground(f, stop)
	blacklistPrunedWG.Wait()
	redemptionWG.Wait()
	close(stop)
}

func TestBlacklistMultipleViewChanges(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 10
	var nodes []*App
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
		nodes = append(nodes, n)
	}

	var viewChangedWG sync.WaitGroup
	viewChangedWG.Add(len(nodes) - 2)

	var node2RemovedFromBlacklist sync.WaitGroup
	node2RemovedFromBlacklist.Add(len(nodes) + 1)

	var node3RemovedFromBlacklist sync.WaitGroup
	node3RemovedFromBlacklist.Add(len(nodes) + 1)

	start := time.Now()
	for _, n := range nodes {
		l := n.logger.Desugar()
		n.logger = l.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number 3, sequence 2,") {
				viewChangedWG.Done()
			}
			if strings.Contains(entry.Message, "Node 2 was observed sending a prepare") && strings.Contains(entry.Message, "removing it from blacklist") {
				node2RemovedFromBlacklist.Done()
			}
			if strings.Contains(entry.Message, "Node 3 was observed sending a prepare") && strings.Contains(entry.Message, "removing it from blacklist") {
				node3RemovedFromBlacklist.Done()
			}
			return nil
		})).Sugar()
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	startNodes(nodes, network)

	// Put a single decision
	nodes[0].Submit(Request{ID: "genesis", ClientID: "alice"})
	for i := range numberOfNodes {
		<-nodes[i].Delivered
	}

	// Put the next and the next nodes in partition
	nodes[1].Disconnect()
	nodes[2].Disconnect()

	// Accelerate the time until a view change
	done := make(chan struct{})
	var acceleratedNodes []*App
	acceleratedNodes = append(acceleratedNodes, nodes[0])
	acceleratedNodes = append(acceleratedNodes, nodes[2:]...)
	var counter uint64
	accelerateTime(acceleratedNodes, done, true, true, &counter)

	// Wait for two view changes
	viewChangedWG.Wait()
	close(done)

	// Rotate the leader and ensure the view doesn't change, which proves
	// nodes 2 and 3 never became the leader again
	for j := range numberOfNodes {
		nodes[5].Submit(Request{ID: fmt.Sprintf("%d", j), ClientID: "alice"})
		for i := 3; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}
	}

	// Ensure we remain on view 2, and that nodes 2,3 are in the blacklist
	md := &smartbftprotos.ViewMetadata{}
	err = proto.Unmarshal(nodes[5].lastDecision.Proposal.Metadata, md)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), md.ViewId)
	assert.Equal(t, []uint64{2, 3}, md.BlackList)

	// Next, re-connect nodes 2,3
	nodes[1].Connect()
	nodes[2].Connect()

	// Accelerate time to wait for them to synchronize
	done = make(chan struct{})
	accelerateTime(nodes, done, true, true, &counter)
	for range numberOfNodes {
		<-nodes[1].Delivered
		<-nodes[2].Delivered
	}
	close(done)

	// Rotate the leader and ensure the view doesn't change,
	stop := make(chan struct{})
	f := func() {
		txID := make([]byte, 15)
		rand.Read(txID)
		nodes[3].Submit(Request{ID: hex.EncodeToString(txID), ClientID: "alice"})
		for i := range nodes {
			select {
			case <-nodes[i].Delivered:
			case <-stop:
				return
			}
		}
		md := &smartbftprotos.ViewMetadata{}
		err = proto.Unmarshal(nodes[3].lastDecision.Proposal.Metadata, md)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), md.ViewId)
	}
	go doInBackground(f, stop)
	node2RemovedFromBlacklist.Wait()
	node3RemovedFromBlacklist.Wait()
	close(stop)
}

func TestNodeInFlightThenViewChange(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)

	var blockCommits uint32

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			if msg.GetCommit() == nil {
				return false
			}
			return atomic.LoadUint32(&blockCommits) == 1
		})
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	atomic.StoreUint32(&blockCommits, 1)

	nodes[0].Submit(Request{ID: "first request", ClientID: "alice"})

	// Ensure no one commits a block because of not enough commits gathered
	var wg sync.WaitGroup
	wg.Add(numberOfNodes)

	for i := range numberOfNodes {
		go func(deliverChannel chan *AppRecord) {
			defer wg.Done()
			select {
			case <-deliverChannel:
				assert.Fail(t, "should not have committed because first commit message should have been lost")
			case <-time.After(time.Second):
			}
		}(nodes[i].Delivered)
	}

	wg.Wait()
	atomic.StoreUint32(&blockCommits, 0)

	nodes[len(nodes)-1].Disconnect() // Last node in partition

	// Submit second request to all nodes to trigger view change
	nodes[0].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[1].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "second request", ClientID: "alice"})

	for i := range numberOfNodes - 1 {
		<-nodes[i].Delivered // first request
		<-nodes[i].Delivered // second request
	}

	// Submit a third request to one of the nodes
	nodes[0].Submit(Request{ID: "third request", ClientID: "alice"})

	for i := range numberOfNodes - 1 {
		<-nodes[i].Delivered
	}

	// Reconnect the offline node and wait for it to sync
	nodes[len(nodes)-1].Connect()
	<-nodes[len(nodes)-1].Delivered
	<-nodes[len(nodes)-1].Delivered
	<-nodes[len(nodes)-1].Delivered

	// Disconnect leader
	nodes[1].Disconnect()

	// Submit fourth request to all nodes to trigger view change
	nodes[0].Submit(Request{ID: "fourth request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "fourth request", ClientID: "alice"})
	nodes[3].Submit(Request{ID: "fourth request", ClientID: "alice"})

	// Wait for a view change and a commit
	<-nodes[0].Delivered
	<-nodes[2].Delivered
	<-nodes[3].Delivered
}

func TestNodeCommitTheRestPrepareAndCommittedNodeCrashesThenRecovers(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)

	var blockCommits uint32

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		// All but the leader don't receive any commits
		if i != 1 {
			n.LoseMessages(func(msg *smartbftprotos.Message) bool {
				if msg.GetCommit() == nil {
					return false
				}
				return atomic.LoadUint32(&blockCommits) == 1
			})
		}

		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	atomic.StoreUint32(&blockCommits, 1)

	nodes[0].Submit(Request{ID: "first request", ClientID: "alice"})

	firstReqs := make(map[string]struct{})

	// Ensure the leader commits
	cmt := <-nodes[0].Delivered
	firstReqs[string(cmt.Batch.toBytes())] = struct{}{}

	// Disconnect the leader
	nodes[0].Disconnect()

	// Unblock commits
	atomic.StoreUint32(&blockCommits, 0)

	// Submit second request to all nodes to trigger view change
	nodes[1].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[3].Submit(Request{ID: "second request", ClientID: "alice"})

	secondReqs := make(map[string]struct{})
	// Nodes should commit both decisions
	for i := 1; i < numberOfNodes; i++ {
		first := <-nodes[i].Delivered  // first request
		second := <-nodes[i].Delivered // second request
		firstReqs[string(first.Batch.toBytes())] = struct{}{}
		secondReqs[string(second.Batch.toBytes())] = struct{}{}
	}

	// Reconnect the old leader
	nodes[0].Connect()

	// Wait for the old leader to sync
	cmt = <-nodes[0].Delivered // second request
	secondReqs[string(cmt.Batch.toBytes())] = struct{}{}

	// Disconnect the new leader
	nodes[1].Disconnect()

	// Submit a third request to force view change
	nodes[0].Submit(Request{ID: "third request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "third request", ClientID: "alice"})
	nodes[3].Submit(Request{ID: "third request", ClientID: "alice"})

	// Wait for remaining nodes to commit
	<-nodes[0].Delivered
	<-nodes[2].Delivered
	<-nodes[3].Delivered

	assert.Len(t, firstReqs, 1)
	assert.Len(t, secondReqs, 1)
}

func TestNodePreparesTheRestInPartitionThenPartitionHeals(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)

	var blockPrepares uint32

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		// All but the leader don't receive any prepares
		if i != 1 {
			n.LoseMessages(func(msg *smartbftprotos.Message) bool {
				if msg.GetPrepare() == nil {
					return false
				}
				return atomic.LoadUint32(&blockPrepares) == 1
			})
		}

		nodes = append(nodes, n)
	}

	// Notice node 0's WAL append when receiving enough prepares
	var wg sync.WaitGroup
	wg.Add(2) // One for pre-prepare reception, one for reception of quorum of prepares

	baseLogger := nodes[0].logger.Desugar()
	nodes[0].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		// Only make not of log appends if we are interested in blocking prepares
		if atomic.LoadUint32(&blockPrepares) == 0 {
			return nil
		}
		if strings.Contains(entry.Message, "LogRecord appended successfully") {
			wg.Done()
		}
		return nil
	})).Sugar()
	nodes[0].Setup()

	startNodes(nodes, network)

	atomic.StoreUint32(&blockPrepares, 1)

	nodes[0].Submit(Request{ID: "first request", ClientID: "alice"})

	// Wait for node 0 to receive both pre-prepare and a quorum of prepares
	wg.Wait()

	// Disconnect node 0
	nodes[0].Disconnect()

	atomic.StoreUint32(&blockPrepares, 0)

	// Submit second request to all nodes to trigger view change
	nodes[1].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[3].Submit(Request{ID: "second request", ClientID: "alice"})

	secondReqs := make(map[string]struct{})
	// Nodes should commit the second request only
	for i := 1; i < numberOfNodes; i++ {
		second := <-nodes[i].Delivered // second request
		secondReqs[string(second.Batch.toBytes())] = struct{}{}
	}

	// Reconnect the old leader
	nodes[0].Connect()

	// Wait for the old leader to sync
	cmt := <-nodes[0].Delivered // second request
	secondReqs[string(cmt.Batch.toBytes())] = struct{}{}

	// Disconnect the new leader
	nodes[1].Disconnect()

	// Submit a third request to force view change
	nodes[0].Submit(Request{ID: "third request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "third request", ClientID: "alice"})
	nodes[3].Submit(Request{ID: "third request", ClientID: "alice"})

	// Wait for remaining nodes to commit
	<-nodes[0].Delivered
	<-nodes[2].Delivered
	<-nodes[3].Delivered

	assert.Len(t, secondReqs, 1)
}

// TestViewChangeAfterTryingToFork - there is some redundancy in the test,
// but it is a reminder of the conditions under which the error was detected.
// This is the first time the 7th node is disconnected and it is connected back in.
// It turned out that at startup one node self-deleted from the consensus.
// This situation can be fixed by adding and configuring backoff between nodes
// (for example, this should be added to fabric,
// the recommended parameters ORDERER_GENERAL_BACKOFF_MAXDELAY: 20s)
// In this test, we disable 3 nodes and destroy the quorum.
// Throw the transaction. And after unsuccessful change of leader (no quorum),
// reconnect 3 nodes. Quorum must be assembled, leader must be successfully
// changed and transaction must be accepted.
func TestViewChangeAfterTryingToFork(t *testing.T) {
	t.Parallel()

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	defer close(done)

	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0, 7)

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()

	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			return false
		})
		n.Setup()
	}

	var once sync.Once

	addFirstViewCh := make(chan struct{}, 6)
	addFirstViewChMap := make(map[uint64]struct{}, 6)
	addFirstSync := sync.RWMutex{}

	viewChange1Ch := make(chan struct{}, 4)
	viewChange1Map := make(map[uint64]struct{}, 4)
	viewChange1Sync := sync.RWMutex{}

	viewChange2Ch := make(chan struct{}, 4)
	viewChange2Map := make(map[uint64]struct{}, 4)
	viewChange2Sync := sync.RWMutex{}

	realViewChangeCh := make(chan struct{}, 7)
	realViewChangeMap := make(map[uint64]struct{}, 7)
	realViewChangeSync := sync.RWMutex{}

	for i := range numberOfNodes {
		id := nodes[i].ID
		baseLogger := nodes[i].Node.app.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		nodes[i].Node.app.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number") &&
				!strings.Contains(entry.Message, "Starting view with number 0") {
				// After returning all the nodes,
				// we fix that a new view has started, but not the very first one
				realViewChangeSync.RLock()
				_, hasVoted := realViewChangeMap[id]
				realViewChangeSync.RUnlock()

				if !hasVoted {
					realViewChangeSync.Lock()
					_, hasVoted = realViewChangeMap[id]
					if !hasVoted {
						realViewChangeMap[id] = struct{}{}
						realViewChangeCh <- struct{}{}
					}
					realViewChangeSync.Unlock()
				}
			}

			if id != 7 {
				// We wait for the node, after no heartbeat, to throw its vote for the leader change.
				// Then we will turn this node off anyway, so that we can't use it to complete the leader change.
				if strings.Contains(entry.Message, "is processing a view change message") &&
					strings.Contains(entry.Message, "from 7") {
					addFirstSync.RLock()
					_, hasVoted := addFirstViewChMap[id]
					addFirstSync.RUnlock()

					if !hasVoted {
						addFirstSync.Lock()
						_, hasVoted = addFirstViewChMap[id]
						if !hasVoted {
							addFirstViewChMap[id] = struct{}{}
							addFirstViewCh <- struct{}{}
						}
						addFirstSync.Unlock()
					}
				}
			}

			if id >= 1 && id <= 4 {
				// All live nodes want to change the leader
				if strings.Contains(entry.Message, "sent view data msg, with next view 1") {
					viewChange1Sync.RLock()
					_, hasVoted := viewChange1Map[id]
					viewChange1Sync.RUnlock()

					if !hasVoted {
						viewChange1Sync.Lock()
						_, hasVoted = viewChange1Map[id]
						if !hasVoted {
							viewChange1Map[id] = struct{}{}
							viewChange1Ch <- struct{}{}
						}
						viewChange1Sync.Unlock()
					}
				}
				// All live nodes have begun the leader change procedure
				if strings.Contains(entry.Message, "started view change, last view is 1") {
					viewChange2Sync.RLock()
					_, hasVoted := viewChange2Map[id]
					viewChange2Sync.RUnlock()

					if !hasVoted {
						viewChange2Sync.Lock()
						_, hasVoted = viewChange2Map[id]
						if !hasVoted {
							viewChange2Map[id] = struct{}{}
							viewChange2Ch <- struct{}{}
						}
						viewChange2Sync.Unlock()
					}
				}
			}

			if id == 7 {
				// Emulation of node removal from the consensus in the absence of heartbeat
				if strings.Contains(entry.Message, "Heartbeat timeout expired") {
					once.Do(func() {
						nodes[6].Connect()
					})
				}
			}
			return nil
		})).Sugar()
	}

	startNodes(nodes, network)

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	// Emulation of node removal from the consensus in the absence of heartbeat
	nodes[6].Disconnect()

	// Expect all nodes to see the 7th node's proposal to change the leader
	for range 6 {
		<-addFirstViewCh
	}

	// Breaking consensus for all operations: performing a transaction and changing a view
	nodes[6].Disconnect()
	nodes[5].Disconnect()
	nodes[4].Disconnect()

	for i := range numberOfNodes - 3 {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	// Expect the desire of all living nodes to change the leader
	fail := time.After(1 * time.Minute)
	for range 4 {
		select {
		case <-viewChange1Ch:
		case <-fail:
			t.Fatal("Didn't change view 1")
		}
	}

	// Expect leader changes to begin from all live nodes
	fail = time.After(1 * time.Minute)
	for range 4 {
		select {
		case <-viewChange2Ch:
		case <-fail:
			t.Fatal("Didn't change view 2")
		}
	}

	// Return the nodes to the consensus
	nodes[6].Connect()
	nodes[5].Connect()
	nodes[4].Connect()

	// Waiting for a real change of leader and view
	fail = time.After(realViewChangeTimeout)
	for range 7 {
		select {
		case <-realViewChangeCh:
		case <-fail:
			t.Fatal("Didn't change real view")
		}
	}

	data := make([]*AppRecord, 0, 7)
	storeI := -1
	fail = time.After(1 * time.Minute)
ExternalLoop:
	for i := range numberOfNodes {
		select {
		case d := <-nodes[i].Delivered:
			data = append(data, d)
		case <-fail:
			storeI = i
			break ExternalLoop
		}
	}

	if storeI != -1 {
		for i := range numberOfNodes {
			nodes[i].Submit(Request{ID: "2", ClientID: "bob"})
		}

		for i := storeI; i < numberOfNodes; i++ {
			d := <-nodes[i].Delivered
			data = append(data, d)
		}
	}

	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	if storeI == -1 {
		return
	}

	data1 := make([]*AppRecord, 0, 7)
	fail = time.After(1 * time.Minute)
	for i := range numberOfNodes {
		select {
		case d := <-nodes[i].Delivered:
			data1 = append(data1, d)
		case <-fail:
			t.Fatal("Didn't deliver two msg")
		}
	}

	for i := range numberOfNodes - 1 {
		assert.Equal(t, data1[i], data1[i+1])
	}
}

func TestFetchStateWhenSyncReturnsPrevView(t *testing.T) {
	// Scenario: A node (node 2) becomes the leader but disconnects inducing a second view change.
	// When it reconnects, with no transactions available, it syncs after receiving multiple heartbeat responses.
	// However, the Synchronizer returns with an old view, and so for the node to catch up it needs to fetch the state.

	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.Setup()
	}

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(numberOfNodes - 1)

	syncWG := sync.WaitGroup{}
	syncWG.Add(1)

	ignoreWG := sync.WaitGroup{}
	ignoreWG.Add(1)
	ignoreOnce := sync.Once{}

	stateWG := sync.WaitGroup{}
	stateWG.Add(1)

	for _, n := range nodes {
		id := n.ID
		baseLogger := n.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		n.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "ViewChanged") {
				viewChangeWG.Done()
			}

			if strings.Contains(entry.Message, "Received HeartBeatResponse triggered a call to HeartBeatEventHandler Sync") {
				syncWG.Done()
			}

			if strings.Contains(entry.Message, "Monitor already called Sync, ignoring HeartBeatResponse") {
				if id == 2 {
					ignoreOnce.Do(func() {
						ignoreWG.Done()
					})
				}
			}

			if strings.Contains(entry.Message, "collected state with view 2 and sequence 2") {
				stateWG.Done()
			}

			if strings.Contains(entry.Message, "but I am in view 1 and seq 2") {
				panic("Didn't fetch state")
			}

			return nil
		})).Sugar()
	}

	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition

	// Accelerate the time until a view change because of heartbeat timeout
	var counter uint64
	accelerateTime(nodes, done, true, false, &counter)

	viewChangeWG.Wait()
	close(done)

	nodes[0].Connect() // connect after view changed

	done = make(chan struct{})
	accelerateTime(nodes, done, true, false, &counter)

	syncWG.Wait()
	close(done)

	syncWG.Add(1)
	viewChangeWG.Add(3)
	nodes[1].Disconnect() // leader in partition

	done = make(chan struct{})
	accelerateTime(nodes, done, true, false, &counter)

	viewChangeWG.Wait()
	close(done)

	nodes[1].Connect()

	done = make(chan struct{})
	accelerateTime(nodes, done, true, false, &counter)

	syncWG.Wait()
	ignoreWG.Wait()
	close(done)

	stateWG.Wait()

	for i := range numberOfNodes {
		nodes[i].Submit(Request{ID: "2", ClientID: "alice"}) // submit to all nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 2 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderStopSendHeartbeat(t *testing.T) {
	t.Parallel()

	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0, 4)

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()

	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})

	prepareFirstCh := make(chan struct{}, 2)
	prepareFirstMap := make(map[uint64]struct{}, 2)
	prepareFirstSync := sync.RWMutex{}

	realViewChangeCh := make(chan struct{}, 4)
	realViewChangeMap := make(map[uint64]struct{}, 4)
	realViewChangeSync := sync.RWMutex{}

	for i := range numberOfNodes {
		id := nodes[i].ID
		baseLogger := nodes[i].Node.app.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		nodes[i].Node.app.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number") &&
				!strings.Contains(entry.Message, "Starting view with number 0") {
				realViewChangeSync.RLock()
				_, hasVoted := realViewChangeMap[id]
				realViewChangeSync.RUnlock()

				if !hasVoted {
					realViewChangeSync.Lock()
					_, hasVoted = realViewChangeMap[id]
					if !hasVoted {
						realViewChangeMap[id] = struct{}{}
						realViewChangeCh <- struct{}{}
					}
					realViewChangeSync.Unlock()
				}
			}

			if (id == 1 || id == 2) &&
				strings.Contains(entry.Message, "prepare") {
				prepareFirstSync.RLock()
				_, hasVoted := prepareFirstMap[id]
				prepareFirstSync.RUnlock()

				if !hasVoted {
					prepareFirstSync.Lock()
					_, hasVoted = prepareFirstMap[id]
					if !hasVoted {
						prepareFirstMap[id] = struct{}{}
						prepareFirstCh <- struct{}{}
					}
					prepareFirstSync.Unlock()
				}
			}

			return nil
		})).Sugar()
	}

	startNodes(nodes, network)

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	nodes[2].Disconnect()
	nodes[3].Disconnect()

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	fail := time.After(1 * time.Minute)
	for range 2 {
		select {
		case <-prepareFirstCh:
		case <-fail:
			t.Fatal("Didn't change view 1")
		}
	}

	nodes[2].Connect()
	nodes[3].Connect()

	fail = time.After(realViewChangeTimeout)
	for range 4 {
		select {
		case <-realViewChangeCh:
		case <-fail:
			t.Fatal("Didn't change real view")
		}
	}

	close(done)

	data := make([]*AppRecord, 0, 4)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}

	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

// TestTryCommittedSequenceTwice - is a modified copy of TestViewChangeAfterTryingToFork,
// because it periodically recorded an error.
// The difference from the parent is that there is work with new channels:
// twiceDeliverBeginCh, deliverControlCh and deliverViewChangerCh,
// After the quorum is restored, two "change of viewpoint" processes take place in parallel,
// during which a transaction delivery and an attempt to deliver a transaction by the new leader can take place.
// With the help of channels twiceDeliverBeginCh, deliverControlCh and deliverViewChangerCh
// at the right moments of time, suspended or triggered the corresponding goroutines,
// so that double delivery can take place
func TestTryCommittedSequenceTwice(t *testing.T) {
	t.Parallel()

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	defer close(done)

	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer func() {
		_ = os.RemoveAll(testDir)
	}()

	numberOfNodes := 7
	nodes := make([]*App, 0, 7)

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		// n.Consensus.Application
		nodes = append(nodes, n)
	}

	start := time.Now()

	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			return false
		})
		n.Setup()
	}

	var once sync.Once

	addFirstViewCh := make(chan struct{}, 6)
	addFirstViewChMap := make(map[uint64]struct{}, 6)
	addFirstSync := sync.RWMutex{}

	viewChange1Ch := make(chan struct{}, 4)
	viewChange1Map := make(map[uint64]struct{}, 4)
	viewChange1Sync := sync.RWMutex{}

	viewChange2Ch := make(chan struct{}, 4)
	viewChange2Map := make(map[uint64]struct{}, 4)
	viewChange2Sync := sync.RWMutex{}

	realViewChangeCh := make(chan struct{}, 7)
	realViewChangeMap := make(map[uint64]struct{}, 7)
	realViewChangeSync := sync.RWMutex{}

	twiceDeliverBeginCh := make(chan struct{}, 2)
	deliverControlCh := make(chan struct{})
	deliverViewChangerCh := make(chan struct{})
	go func() {
		// 2. [twice] Waiting for the moment when both goroutines are ready to deliver the transaction
		<-twiceDeliverBeginCh
		<-twiceDeliverBeginCh
		// 3. [twice] Started delivery in goroutine viewchanger
		close(deliverViewChangerCh)
	}()

	for i := range numberOfNodes {
		id := nodes[i].ID
		baseLogger := nodes[i].Node.app.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		nodes[i].Node.app.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number") &&
				!strings.Contains(entry.Message, "Starting view with number 0") {
				// After returning all the nodes,
				// we fix that a new view has started, but not the very first one.
				realViewChangeSync.RLock()
				_, hasVoted := realViewChangeMap[id]
				realViewChangeSync.RUnlock()

				if !hasVoted {
					realViewChangeSync.Lock()
					_, hasVoted = realViewChangeMap[id]
					if !hasVoted {
						realViewChangeMap[id] = struct{}{}
						realViewChangeCh <- struct{}{}
					}
					realViewChangeSync.Unlock()
				}
			}

			if id != 7 {
				// We wait for the node, after no heartbeat, to throw its vote for the leader change.
				// Then we will turn this node off anyway, so that we can't use it to complete the leader change.
				if strings.Contains(entry.Message, "is processing a view change message") &&
					strings.Contains(entry.Message, "from 7") {
					addFirstSync.RLock()
					_, hasVoted := addFirstViewChMap[id]
					addFirstSync.RUnlock()

					if !hasVoted {
						addFirstSync.Lock()
						_, hasVoted = addFirstViewChMap[id]
						if !hasVoted {
							addFirstViewChMap[id] = struct{}{}
							addFirstViewCh <- struct{}{}
						}
						addFirstSync.Unlock()
					}
				}
			}

			if id >= 1 && id <= 4 {
				// All live nodes want to change the leader
				if strings.Contains(entry.Message, "sent view data msg, with next view 1") {
					viewChange1Sync.RLock()
					_, hasVoted := viewChange1Map[id]
					viewChange1Sync.RUnlock()

					if !hasVoted {
						viewChange1Sync.Lock()
						_, hasVoted = viewChange1Map[id]
						if !hasVoted {
							viewChange1Map[id] = struct{}{}
							viewChange1Ch <- struct{}{}
						}
						viewChange1Sync.Unlock()
					}
				}
				// All live nodes have begun the leader change procedure
				if strings.Contains(entry.Message, "started view change, last view is 1") {
					viewChange2Sync.RLock()
					_, hasVoted := viewChange2Map[id]
					viewChange2Sync.RUnlock()

					if !hasVoted {
						viewChange2Sync.Lock()
						_, hasVoted = viewChange2Map[id]
						if !hasVoted {
							viewChange2Map[id] = struct{}{}
							viewChange2Ch <- struct{}{}
						}
						viewChange2Sync.Unlock()
					}
				}
			}

			if id == 7 {
				// Emulation of node removal from the consensus in the absence of heartbeat
				if strings.Contains(entry.Message, "Heartbeat timeout expired") {
					once.Do(func() {
						nodes[6].Connect()
					})
				}
			}

			if id == 4 {
				// Suspension of gorutins and their launch to try to implement twice-delivery
				if strings.Contains(entry.Message, "Delivering to app from deliverDecision the last decision proposal") {
					// 1. [twice] Waited by the time the transaction was delivered
					twiceDeliverBeginCh <- struct{}{}
					// 4. [twice] Started delivery in goroutine "viewchanger"
					<-deliverViewChangerCh
				}
				if strings.Contains(entry.Message, "Delivering to app from Decide the last decision proposal") {
					// 1. [twice] Waited by the time the transaction was delivered
					twiceDeliverBeginCh <- struct{}{}
					// 4. [twice] Started delivery in goroutine "viewchanger"
					<-deliverViewChangerCh
				}
				if strings.Contains(entry.Message, "Delivering end to app from deliverDecision the last decision proposal") {
					// 5. [twice] "Viewchanger" delivered the transaction, let's pass the baton on
					close(deliverControlCh)
				}
				if strings.Contains(entry.Message, "Delivering end to app from Decide the last decision proposal") {
					// 5. [twice] "Viewchanger" delivered the transaction, let's pass the baton on
					close(deliverControlCh)
				}
				if strings.Contains(entry.Message, "Delivering to app from Controller decide the last decision proposal") {
					// 1. [twice] Waited by the time the transaction was delivered
					twiceDeliverBeginCh <- struct{}{}
					// 6. [twice] Started delivery in goroutine "control"
					<-deliverControlCh
				}
			}
			return nil
		})).Sugar()
	}

	startNodes(nodes, network)

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	// Emulation of node removal from the consensus in the absence of heartbeat
	nodes[6].Disconnect()

	// Expect all nodes to see the 7th node's proposal to change the leader
	for range 6 {
		<-addFirstViewCh
	}

	// Breaking consensus for all operations: performing a transaction and changing a view
	nodes[6].Disconnect()
	nodes[5].Disconnect()
	nodes[4].Disconnect()

	for i := range numberOfNodes - 3 {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	// Expect the desire of all living nodes to change the leader
	fail := time.After(1 * time.Minute)
	for range 4 {
		select {
		case <-viewChange1Ch:
		case <-fail:
			t.Fatal("Didn't change view 1")
		}
	}

	// Expect leader changes to begin from all live nodes
	fail = time.After(1 * time.Minute)
	for range 4 {
		select {
		case <-viewChange2Ch:
		case <-fail:
			t.Fatal("Didn't change view 2")
		}
	}

	// Return the nodes to the consensus
	nodes[6].Connect()
	nodes[5].Connect()
	nodes[4].Connect()

	// Waiting for a real change of leader and view
	fail = time.After(realViewChangeTimeout)
	for range 7 {
		select {
		case <-realViewChangeCh:
		case <-fail:
			t.Fatal("Didn't change real view")
		}
	}

	data := make([]*AppRecord, 0, 7)
	storeI := -1
	fail = time.After(1 * time.Minute)
ExternalLoop:
	for i := range numberOfNodes {
		select {
		case d := <-nodes[i].Delivered:
			data = append(data, d)
		case <-fail:
			storeI = i
			break ExternalLoop
		}
	}

	if storeI != -1 {
		for i := range numberOfNodes {
			nodes[i].Submit(Request{ID: "2", ClientID: "bob"})
		}

		for i := storeI; i < numberOfNodes; i++ {
			d := <-nodes[i].Delivered
			data = append(data, d)
		}
	}

	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	if storeI == -1 {
		return
	}

	data1 := make([]*AppRecord, 0, 7)
	fail = time.After(1 * time.Minute)
	for i := range numberOfNodes {
		select {
		case d := <-nodes[i].Delivered:
			data1 = append(data1, d)
		case <-fail:
			t.Fatal("Didn't deliver two msg")
		}
	}

	for i := range numberOfNodes - 1 {
		assert.Equal(t, data1[i], data1[i+1])
	}
}

// Decide and abort view at the same time
func TestDecideAndAbortViewAtSameTime(t *testing.T) {
	t.Parallel()

	done := make(chan struct{})
	defer close(done)

	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.LeaderHeartbeatTimeout = 5 * time.Second
		n.Consensus.Config.LeaderHeartbeatCount = 2
		n.Consensus.Config.ViewChangeTimeout = 20 * time.Second
		nodes = append(nodes, n)
	}

	var (
		once  sync.Once
		once1 sync.Once
	)
	leaderComplaneCh := make(chan struct{})
	abortingCh := make(chan struct{}, 1)
	nextSeqCh := make(chan struct{}, 1)
	nextStep := make(chan struct{}, 1)

	baseLogger := nodes[3].logger.Desugar()
	nodes[3].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Heartbeat timeout expired") {
			once.Do(func() {
				leaderComplaneCh <- struct{}{}
			})
		}
		if strings.Contains(entry.Message, "Aborting current view with number") {
			once1.Do(func() {
				abortingCh <- struct{}{}
				<-nextStep
			})
		}
		if strings.Contains(entry.Message, "Sequence: 2-->3") {
			nextSeqCh <- struct{}{}
			<-nextStep
		}
		return nil
	})).Sugar()

	nodes[3].Setup()

	startNodes(nodes, network)

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader
	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})
	<-nextSeqCh

	// Emulation of node removal from the consensus in the absence of heartbeat
	nodes[3].Disconnect()
	<-leaderComplaneCh
	nodes[3].Connect()

	<-abortingCh

	close(nextStep)
	data = make([]*AppRecord, 0, 4)
	fail := time.After(15 * time.Second)
	for i := range numberOfNodes {
		select {
		case d := <-nodes[i].Delivered:
			data = append(data, d)
		case <-fail:
			buf := make([]byte, 1<<16)
			runtime.Stack(buf, true)
			fmt.Printf("%s", buf)
			t.Fatal("Didn't deliver two msg")
		}
	}

	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func doInBackground(f func(), stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
			f()
		}
	}
}

func countCommittedBatches(n *App) int {
	var numBatchesCreated int
	for {
		select {
		case <-n.Delivered:
			numBatchesCreated++
		case <-time.After(time.Millisecond * 500):
			return numBatchesCreated
		}
	}
}

func requestIDFromBatch(record *AppRecord) int {
	n, _ := strconv.ParseInt(requestFromBytes(record.Batch.Requests[0]).ID, 10, 32)
	return int(n)
}

func waitForCatchup(targetReqID int, out chan *AppRecord) bool {
	for {
		select {
		case record := <-out:
			if requestIDFromBatch(record) == targetReqID {
				return true
			}
		case <-time.After(time.Millisecond * 100):
			return false
		}
	}
}

func accelerateTime(nodes []*App, done chan struct{}, heartbeatTime, viewChangeTime bool, counter *uint64) {
	go func() {
		for {
			atomic.AddUint64(counter, 1)
			newTime := time.Now().Add(time.Second * time.Duration(2*atomic.LoadUint64(counter)))
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 100):
				for _, n := range nodes {
					if heartbeatTime {
						select {
						case n.heartbeatTime <- newTime:
						default:
						}
					}
					if viewChangeTime {
						select {
						case n.viewChangeTime <- newTime:
						default:
						}
					}
				}
			}
		}
	}()
}

func startNodes(nodes []*App, network *Network) {
	for _, n := range nodes {
		if err := n.Consensus.Start(); err != nil {
			n.logger.Panicf("Consensus returned an error : %v", err)
		}
	}
	network.StartServe()
}

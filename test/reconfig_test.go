package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestBasicReconfig(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	decisions := uint64(1)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, decisions)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	newConfig := fastConfig
	newConfig.CollectTimeout = fastConfig.CollectTimeout * 2
	newConfig.LeaderRotation = true
	newConfig.DecisionsPerLeader = decisions

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestPoolPrunedAfterReconfig(t *testing.T) {
	// A follower holds a request in its pool that becomes invalid once a reconfig is delivered,
	// so the request must be pruned from the pool after the reconfig.

	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	// Long enough for the follower to never forward the request to the leader during the test
	forwardTimeout := time.Minute

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.RequestForwardTimeout = forwardTimeout
		n.Consensus.Config.RequestComplainTimeout = 2 * forwardTimeout
		n.clientsToRevoke = []string{"bob"}
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	follower := nodes[1]
	follower.Submit(Request{ID: "1", ClientID: "bob"})
	assert.Equal(t, 1, follower.Consensus.Pool.Size())

	newConfig := fastConfig
	newConfig.RequestForwardTimeout = forwardTimeout
	newConfig.RequestComplainTimeout = 2 * forwardTimeout

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	assert.Eventually(t, func() bool {
		return follower.Consensus.Pool.Size() == 0
	}, 10*time.Second, 100*time.Millisecond)

	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestBasicAddNodes(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	decisions := uint64(1)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, decisions)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data1 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data1 = append(data1, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data1[i], data1[i+1])
	}

	newNode1 := newNode(5, network, t.Name(), testDir, true, decisions)
	newNode2 := newNode(6, network, t.Name(), testDir, true, decisions)

	newConfig := fastConfig
	newConfig.LeaderRotation = true
	newConfig.DecisionsPerLeader = decisions

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data2 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data2 = append(data2, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data2[i], data2[i+1])
	}

	nodes = append(nodes, newNode1, newNode2)
	startNodes(nodes[4:], network)

	d1 := <-nodes[4].Delivered
	d2 := <-nodes[5].Delivered
	assert.Equal(t, data1[0], d1)
	assert.Equal(t, data1[0], d2)
	d1 = <-nodes[4].Delivered
	d2 = <-nodes[5].Delivered
	assert.Equal(t, data2[0], d1)
	assert.Equal(t, data2[0], d2)

	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	numberOfNodes = 6
	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestBasicRemoveNodes(t *testing.T) {
	// In the beginning there are 7 nodes and the quorum size is 5,
	// after removing 3 nodes the new quorum size should be 3,
	// so transactions can be committed after the reconfiguration only if it was successful.

	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	decisions := uint64(1)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, decisions)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	newConfig := fastConfig
	newConfig.LeaderRotation = true
	newConfig.DecisionsPerLeader = decisions

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     []int64{1, 2, 3, 4},
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[4].Disconnect()
	nodes[5].Disconnect()
	nodes[6].Disconnect()

	numberOfNodes = 4
	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestAddRemoveNodes(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	decisions := uint64(1)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, decisions)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data1 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data1 = append(data1, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data1[i], data1[i+1])
	}

	for i := 5; i <= 10; i++ {
		newNode := newNode(uint64(i), network, t.Name(), testDir, true, decisions)
		nodes = append(nodes, newNode)
	}

	newConfig := fastConfig
	newConfig.LeaderRotation = true
	newConfig.DecisionsPerLeader = decisions

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data2 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data2 = append(data2, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data2[i], data2[i+1])
	}

	startNodes(nodes[4:], network)

	numberOfNodes = 10
	// make sure both decisions were delivered by the new nodes
	data := make([]*AppRecord, 0)
	for i := 4; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 4 {
		assert.Equal(t, data[i], data1[0])
	}
	data = make([]*AppRecord, 0)
	for i := 4; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 4 {
		assert.Equal(t, data[i], data2[0])
	}

	nodes[9].Submit(Request{ID: "11", ClientID: "alice"})
	data3 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data3 = append(data3, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data3[i], data3[i+1])
	}

	nodesToRemove := 4
	// remove nodes one by one, starting from first
	for r := 1; r <= nodesToRemove; r++ {
		currentNodes := make([]int64, 0)
		for i := r + 1; i <= numberOfNodes; i++ {
			currentNodes = append(currentNodes, int64(i))
		}

		nodes[9].Submit(Request{
			ClientID: "reconfig",
			ID:       fmt.Sprintf("%d", 50+r),
			Reconfig: Reconfig{
				InLatestDecision: true,
				CurrentNodes:     currentNodes,
				CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
			},
		})

		data = make([]*AppRecord, 0)
		for i := r - 1; i < numberOfNodes; i++ {
			d := <-nodes[i].Delivered
			data = append(data, d)
		}
		for i := range numberOfNodes - r {
			assert.Equal(t, data[i], data[i+1])
		}

		nodes[r-1].Disconnect()
	}

	nodes[9].Submit(Request{ID: "12", ClientID: "alice"})
	data = make([]*AppRecord, 0)
	for i := nodesToRemove; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - nodesToRemove - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestAddRemoveAddNodes(t *testing.T) {
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
		n.Mute()
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data1 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data1 = append(data1, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data1[i], data1[i+1])
	}

	newNode := newNode(uint64(5), network, t.Name(), testDir, false, 0)
	nodes = append(nodes, newNode)

	newConfig := fastConfig
	newConfig.LeaderRotation = false
	newConfig.DecisionsPerLeader = 0

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     []int64{1, 2, 3, 4, 5},
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data2 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data2 = append(data2, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data2[i], data2[i+1])
	}

	startNodes(nodes[4:], network)

	<-nodes[4].Delivered
	d := <-nodes[4].Delivered

	assert.Equal(t, "10", requestFromBytes(d.Batch.Requests[0]).ID)

	nodes[4].Disconnect()

	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	data3 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data3 = append(data3, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data3[i], data3[i+1])
	}

	nodes[0].Submit(Request{ID: "12", ClientID: "alice"})
	data4 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data4 = append(data4, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data4[i], data4[i+1])
	}

	nodes[0].Submit(Request{ID: "13", ClientID: "alice"})
	data5 := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data5 = append(data5, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data5[i], data5[i+1])
	}

	nodes[4].logger.Infof("--------------------------------")
	nodes[4].Connect()

	<-nodes[4].Delivered

	<-nodes[4].Delivered

	<-nodes[4].Delivered

	nodes[4].logger.Infof("--------------------------------")

	nodes[0].Submit(Request{ID: "14", ClientID: "alice"})
	data6 := make([]*AppRecord, 0)

	fail := time.After(1 * time.Minute)
	for i := range numberOfNodes + 1 {
		select {
		case d := <-nodes[i].Delivered:
			data6 = append(data6, d)
		case <-fail:
			t.Fatal("Didn't get delivered")
		}
	}
	for i := range numberOfNodes {
		assert.Equal(t, data6[i], data6[i+1])
	}
}

func TestViewChangeAfterReconfig(t *testing.T) {
	// The initial nodes ids are {2,3,4,5}
	// After reconfiguration a fifth node is added with id 1
	// however this node is disconnected, and so a view change is needed

	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 2; i <= numberOfNodes+1; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	newNode := newNode(1, network, t.Name(), testDir, false, 0)
	nodes = append(nodes, newNode)
	startNodes(nodes[4:], network)
	nodes[4].Disconnect()

	newConfig := fastConfig
	newConfig.LeaderRotation = false
	newConfig.DecisionsPerLeader = 0

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data = make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := range numberOfNodes {
		nodes[i].Submit(Request{ID: "11", ClientID: "alice"})
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

func TestAddNodeAfterManyRotations(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir, err := os.MkdirTemp("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	decisions := uint64(1)

	numberOfNodes := 4
	blocksNum := 4 + 2
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, decisions)
		nodes = append(nodes, n)
	}
	startNodes(nodes, network)

	// deliver many blocks and rotate after each block
	blocks := make([]*AppRecord, 0)
	for j := 1; j <= blocksNum; j++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", j), ClientID: "alice"})
		data := make([]*AppRecord, 0)
		for i := range numberOfNodes {
			d := <-nodes[i].Delivered
			data = append(data, d)
		}
		for i := range numberOfNodes - 1 {
			assert.Equal(t, data[i], data[i+1])
		}
		blocks = append(blocks, data[0])
	}

	// add a new node
	newNode := newNode(5, network, t.Name(), testDir, true, decisions)

	newConfig := fastConfig
	newConfig.LeaderRotation = true
	newConfig.DecisionsPerLeader = decisions

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	reconfigBlock := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		reconfigBlock = append(reconfigBlock, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, reconfigBlock[i], reconfigBlock[i+1])
	}

	nodes = append(nodes, newNode)
	startNodes(nodes[4:], network)

	// make sure the new node synced with the many delivered blocks
	for j := 1; j <= blocksNum; j++ {
		d := <-nodes[4].Delivered
		assert.Equal(t, blocks[j-1], d)
	}

	// make sure the new node synced with the reconfig block
	d := <-nodes[4].Delivered
	assert.Equal(t, reconfigBlock[0], d)

	// submit another transaction and make sure all got it
	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	numberOfNodes = 5
	data := make([]*AppRecord, 0)
	for i := range numberOfNodes {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := range numberOfNodes - 1 {
		assert.Equal(t, data[i], data[i+1])
	}
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// viewTracker records, per node, which view change steps it reached. It is used to see where a
// split cluster stops making progress: whether a node ever asks to change view, whether it
// gathers the votes for it, whether the leader of that view ever gets its view data, and why it
// synchronized.
type viewTracker struct {
	mu       sync.Mutex
	attempts map[uint64]int
	quorums  map[uint64]int
	viewData map[uint64]int
	syncs    map[uint64]int
	views    map[uint64]uint64
	// proposals is the furthest sequence each node processed a proposal at. The leader broadcasts
	// the pre-prepare while processing the proposal, and it is broadcast only once, so this is
	// what tells a test whether a node that joins a view later has already missed it.
	proposals map[uint64]uint64
}

func newViewTracker() *viewTracker {
	return &viewTracker{
		attempts:  make(map[uint64]int),
		quorums:   make(map[uint64]int),
		viewData:  make(map[uint64]int),
		syncs:     make(map[uint64]int),
		views:     make(map[uint64]uint64),
		proposals: make(map[uint64]uint64),
	}
}

// hook returns a zap hook that feeds the log of a node into the tracker. The messages are the
// ones that show the progress of the view change protocol step by step.
func (vt *viewTracker) hook(id uint64) func(zapcore.Entry) error {
	return func(entry zapcore.Entry) error {
		msg := entry.Message

		switch {
		case strings.Contains(msg, "started view change, last view is"):
			vt.bump(vt.attempts, id)
		case strings.Contains(msg, "ViewChanged, the new view is"):
			if n, err := strconv.ParseUint(after(msg, "ViewChanged, the new view is"), 10, 64); err == nil {
				vt.setView(id, n)
			}
		case strings.Contains(msg, "Starting view with number"):
			// a node that learns about a newer view by syncing does not announce a view
			// change, it starts the view directly, and would otherwise be reported as still
			// being in the view it was left behind in
			if n, err := strconv.ParseUint(after(msg, "Starting view with number"), 10, 64); err == nil {
				vt.setView(id, n)
			}
		case strings.Contains(msg, "Processed proposal with seq"):
			if n, err := strconv.ParseUint(after(msg, "Processed proposal with seq"), 10, 64); err == nil {
				vt.setProposal(id, n)
			}
		case strings.Contains(msg, "sent view data msg"):
			vt.bump(vt.viewData, id)
		case strings.Contains(msg, "got a quorum of viewData messages"):
			vt.bump(vt.quorums, id)
		case strings.Contains(msg, "get msg from syncChan"):
			// the controller logs this once for every sync it performs, whichever part of the
			// library asked for it: the view change timeout or a heartbeat saying the cluster
			// is ahead of it
			vt.bump(vt.syncs, id)
		}
		return nil
	}
}

// after returns the remainder of msg once marker has been found, trimmed of anything that is not
// part of the number that follows it.
func after(msg, marker string) string {
	_, after, ok := strings.Cut(msg, marker)
	if !ok {
		return ""
	}
	rest := strings.TrimSpace(after)
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	return rest[:end]
}

func (vt *viewTracker) setView(id, view uint64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	if view > vt.views[id] {
		vt.views[id] = view
	}
}

func (vt *viewTracker) setProposal(id, seq uint64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	if seq > vt.proposals[id] {
		vt.proposals[id] = seq
	}
}

func (vt *viewTracker) bump(m map[uint64]int, id uint64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	m[id]++
}

func (vt *viewTracker) attemptsOf(id uint64) int {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.attempts[id]
}

func (vt *viewTracker) quorumsOf(id uint64) int {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.quorums[id]
}

func (vt *viewTracker) viewDataOf(id uint64) int {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.viewData[id]
}

func (vt *viewTracker) syncsOf(id uint64) int {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.syncs[id]
}

func (vt *viewTracker) proposalOf(id uint64) uint64 {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.proposals[id]
}

// describe returns a short summary of what each node did, to report where the chain broke.
func (vt *viewTracker) describe(ids []uint64) string {
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b, "\n  node %d: view=%d, view change attempts=%d, view data sent=%d, view data quorums=%d, syncs=%d, last proposal=%d",
			id, vt.viewOf(id), vt.attemptsOf(id), vt.viewDataOf(id), vt.quorumsOf(id), vt.syncsOf(id), vt.proposalOf(id))
	}
	return b.String()
}

func (vt *viewTracker) viewOf(id uint64) uint64 {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	return vt.views[id]
}

// attachTracker wires the tracker into the logger of every node, so that what the nodes do is
// visible to the test.
func attachTracker(nodes []*App, vt *viewTracker) {
	for _, n := range nodes {
		id := n.ID
		base := n.logger.Desugar()
		n.logger = base.WithOptions(zap.Hooks(vt.hook(id))).Sugar()
		n.Consensus.Logger = n.logger
	}
}

// newSplitCluster starts a cluster of numberOfNodes nodes with its heartbeat and view change
// timers under the control of the test, and returns the nodes together with a tracker of what
// each of them does.
func newSplitCluster(t *testing.T, numberOfNodes int) (nodes []*App, ids []uint64, vt *viewTracker) {
	t.Helper()

	network := NewNetwork()
	t.Cleanup(network.Shutdown)

	testDir := t.TempDir()

	start := time.Now()
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
		nodes = append(nodes, n)
		ids = append(ids, uint64(i))
	}

	vt = newViewTracker()
	attachTracker(nodes, vt)

	startNodes(nodes, network)

	return nodes, ids, vt
}

// decideFirstRequest makes the cluster agree on a request, so that the split happens on top of
// a state every node shares.
func decideFirstRequest(t *testing.T, nodes []*App) {
	t.Helper()

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})
	for _, n := range nodes {
		select {
		case <-n.Delivered:
		case <-time.After(30 * time.Second):
			t.Fatalf("node %d did not deliver the first transaction", n.ID)
		}
	}
}

// heal restores every link of the cluster.
func heal(nodes []*App, ids []uint64) {
	for _, id := range ids {
		nodes[id-1].Connect()
		for _, other := range ids {
			nodes[id-1].ConnectTo(other)
		}
	}
}

// deliverRequest waits until every node delivered the given request. The Delivered channel is
// buffered and a node may have delivered other records while the views were diverging, so the
// wait has to look for the request submitted by the test rather than for any record that shows
// up.
func deliverRequest(t *testing.T, nodes []*App, ids []uint64, requestID int, vt *viewTracker) {
	t.Helper()

	recovered := make(chan uint64, len(nodes))
	done := make(chan struct{})
	// the receivers outlive this function unless they are stopped, and they would keep
	// reading from the nodes after the test has finished with them
	t.Cleanup(func() { close(done) })

	for _, n := range nodes {
		go func(app *App) {
			timeout := time.After(90 * time.Second)
			for {
				select {
				case record := <-app.Delivered:
					if requestIDFromBatch(record) == requestID {
						select {
						case recovered <- app.ID:
						case <-done:
						}
						return
					}
				case <-timeout:
					return
				case <-done:
					return
				}
			}
		}(n)
	}

	delivered := 0
	got := make([]uint64, 0, len(nodes))
	timeout := time.After(90 * time.Second)
	for delivered < len(nodes) {
		select {
		case id := <-recovered:
			delivered++
			got = append(got, id)
		case <-timeout:
			t.Fatalf("only %d of %d nodes delivered request %d, delivered by %v, missing %v:%s",
				delivered, len(nodes), requestID, got, missing(got, ids), vt.describe(ids))
		}
	}
}

// missing returns the ids that are not in got.
func missing(got, ids []uint64) []uint64 {
	seen := make(map[uint64]bool, len(got))
	for _, id := range got {
		seen[id] = true
	}
	var out []uint64
	for _, id := range ids {
		if !seen[id] {
			out = append(out, id)
		}
	}
	return out
}

// viewsReached waits until every listed node has reached the given view.
func viewsReached(t *testing.T, vt *viewTracker, ids []uint64, view uint64, timeout time.Duration, msg string) bool {
	t.Helper()
	return assert.Eventually(t, func() bool {
		for _, id := range ids {
			if vt.viewOf(id) < view {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, msg)
}

// proposalProcessed waits until the node processed a proposal at the given sequence. The leader
// broadcasts the pre-prepare while processing the proposal and broadcasts it only once, so a node
// that joins the view after this point cannot receive it any more.
func proposalProcessed(t *testing.T, vt *viewTracker, id, seq uint64, timeout time.Duration, msg string) bool {
	t.Helper()
	return assert.Eventually(t, func() bool {
		return vt.proposalOf(id) >= seq
	}, timeout, 100*time.Millisecond, msg)
}

// holdCommits holds back the commit messages the held nodes send to the given targets until
// release is called. A commit message is sent once and is never retransmitted, so without holding
// them back the request would be decided while the nodes that stayed behind are still
// synchronizing, and the state transfer response they collect would already carry the decision
// instead of the proposal in flight.
//
// The hook that MutateSend installs runs synchronously inside SendConsensus, on the goroutine of
// the sender, so a commit that blocks there holds back more than the commit itself: the held nodes
// stop taking part in the view altogether until release, because the goroutine that would send
// their next message is the one parked in the send. Messages that are not commits pass through
// untouched. The heartbeat monitor runs on its own goroutine, so the held nodes keep sending
// heartbeats, and it is those heartbeats that tell the nodes that stayed behind how far the
// cluster got.
func holdCommits(nodes []*App, held []uint64, targets []uint64) (release func()) {
	releaseCh := make(chan struct{})
	for _, id := range held {
		for _, target := range targets {
			if target == id {
				continue
			}
			nodes[id-1].MutateSend(target, func(_ uint64, msg *smartbftprotos.Message) {
				if msg.GetCommit() != nil {
					<-releaseCh
				}
			})
		}
	}
	var once sync.Once
	return func() { once.Do(func() { close(releaseCh) }) }
}

// TestNodeOneSequenceBehindRejoins covers the nodes that are behind by exactly the proposal that
// is in flight.
//
// The cluster is split so that it ends up spread over three views: node 1 keeps view 0, node 2
// keeps view 1, and the other five move on to view 2. The leaders of the views the cluster enters
// while the split deepens - node 2 for view 1 and node 3 for view 2 - move on with the cluster as
// far as it gets, so each view is actually installed and a node is left in it only by being cut
// off while it is in that view.
//
// While node 1 and node 2 are still cut off, the cluster proposes request 2 at sequence 2 in
// view 2. The commits for it are held back, so that the proposal stays in flight while the two
// are let back in. They synchronize and learn that the cluster is in view 2 at sequence 2 - the
// sequence in flight - and start view 2 there. That is all they can learn: the pre-prepare for
// sequence 2 was sent once, while they were still cut off, and is not sent again, so they miss it
// and cannot take part in deciding request 2.
//
// This is what the test pins down: once request 2 is decided, the sequence the cluster reports
// has to move on to the next proposal, so that the heartbeat the leader broadcasts tells the two
// that they are one sequence behind. They then synchronize again and deliver request 2. Before
// the fix, ViewSequences kept the sequence that had just been decided, the heartbeat kept
// reporting it, both nodes believed they were up to date, and request 2 never reached them.
func TestNodeOneSequenceBehindRejoins(t *testing.T) {
	t.Parallel()

	const numberOfNodes = 7

	nodes, ids, vt := newSplitCluster(t, numberOfNodes)
	decideFirstRequest(t, nodes)

	done := make(chan struct{})
	defer close(done)
	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	// Node 1 leads view 0, so it stays in view 0 while cut off: a leader never starts a view
	// change of its own. The other six install view 1, led by node 2.
	nodes[0].Disconnect()
	if !viewsReached(t, vt, ids[1:], 1, 60*time.Second, "the group that moved on did not start view 1") {
		t.Fatalf("the group that moved on did not start view 1:%s", vt.describe(ids))
	}

	// Node 2 leads view 1: cut it off and the other five install view 2, led by node 3.
	nodes[2-1].Disconnect()
	inView2 := []uint64{3, 4, 5, 6, 7}
	if !viewsReached(t, vt, inView2, 2, 60*time.Second, "the five nodes that moved on did not start view 2") {
		t.Fatalf("the five nodes that moved on did not start view 2:%s", vt.describe(ids))
	}

	// the cluster is now spread over three views
	if v := vt.viewOf(1); v != 0 {
		t.Fatalf("node 1 changed view while it was cut off: view=%d:%s", v, vt.describe(ids))
	}
	if v := vt.viewOf(2); v != 1 {
		t.Fatalf("node 2 changed view while it was cut off: view=%d:%s", v, vt.describe(ids))
	}

	t.Logf("state with the nodes spread over views 0, 1 and 2:%s", vt.describe(ids))

	// Hold back the commits of two of the five, so that request 2 can be proposed but not
	// decided while node 1 and node 2 synchronize. Their state transfer has to show them the
	// view and the sequence in flight, not a decided one.
	release := holdCommits(nodes, []uint64{5, 6}, ids)
	defer release()

	// the leader of view 2 proposes request 2 while the two are still cut off
	nodes[3-1].Submit(Request{ID: "2", ClientID: "alice"})
	if !proposalProcessed(t, vt, 3, 2, 60*time.Second, "the leader of view 2 did not propose request 2") {
		t.Fatalf("the leader of view 2 did not propose request 2:%s", vt.describe(ids))
	}

	// let the two back in: they synchronize into view 2 at the sequence in flight, which the
	// state transfer reports while the proposal is held back
	heal(nodes, ids)
	if !viewsReached(t, vt, []uint64{1, 2}, 2, 90*time.Second, "the nodes that stayed behind did not synchronize into view 2") {
		t.Fatalf("the nodes that stayed behind did not synchronize into view 2:%s", vt.describe(ids))
	}

	t.Logf("state after the two joined view 2 at the sequence in flight:%s", vt.describe(ids))

	// let the commits through: the five decide request 2, which the two missed
	release()

	deliverRequest(t, nodes, ids, 2, vt)

	t.Logf("state after the cluster recovered:%s", vt.describe(ids))
}

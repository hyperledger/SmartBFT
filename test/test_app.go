// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"encoding/asn1"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

var fastConfig = types.Configuration{
	RequestBatchMaxCount:          10,
	RequestBatchMaxBytes:          10 * 1024 * 1024,
	RequestBatchMaxInterval:       10 * time.Millisecond,
	IncomingMessageBufferSize:     200,
	RequestPoolSize:               40,
	RequestForwardTimeout:         500 * time.Millisecond,
	RequestComplainTimeout:        2 * time.Second,
	RequestAutoRemoveTimeout:      3 * time.Minute,
	ViewChangeResendInterval:      5 * time.Second,
	ViewChangeTimeout:             1 * time.Minute,
	LeaderHeartbeatTimeout:        1 * time.Minute,
	LeaderHeartbeatCount:          10,
	NumOfTicksBehindBeforeSyncing: 10,
	CollectTimeout:                200 * time.Millisecond,
	LeaderRotation:                false,
	RequestMaxBytes:               10 * 1024,
	RequestPoolSubmitTimeout:      5 * time.Second,
}

// App implements all interfaces required by an application using this library
type App struct {
	ID              uint64
	Delivered       chan *AppRecord
	Consensus       *consensus.Consensus
	Setup           func()
	Node            *Node
	logLevel        zap.AtomicLevel
	latestMD        *smartbftprotos.ViewMetadata
	lastDecision    *types.Decision
	clock           *time.Ticker
	heartbeatTime   chan time.Time
	viewChangeTime  chan time.Time
	secondClock     *time.Ticker
	logger          *zap.SugaredLogger
	metricsProvider metrics.Provider
	lastRecord      lastRecord
	verificationSeq uint64
	messageLost     func(*smartbftprotos.Message) bool
	lock            sync.Mutex
}

type lastRecord struct {
	proposal   types.Proposal
	signatures []types.Signature
}

// Mute mutes the log
func (a *App) Mute() {
	a.logLevel.SetLevel(zapcore.PanicLevel)
}

// UnMute unmutes the log
func (a *App) UnMute() {
	a.logLevel.SetLevel(zapcore.DebugLevel)
}

// Submit submits the client request
func (a *App) Submit(req Request) {
	a.Consensus.SubmitRequest(req.ToBytes())
}

// Sync synchronizes and returns the latest decision
func (a *App) Sync() types.SyncResponse {
	a.Node.probabilityLock.RLock()
	syncDelay := a.Node.syncDelay
	a.Node.probabilityLock.RUnlock()

	if syncDelay != nil {
		defer func() {
			<-syncDelay
			a.Node.probabilityLock.Lock()
			a.Node.syncDelay = nil
			a.Node.probabilityLock.Unlock()
		}()
	}

	records := a.Node.cb.readAll(a.latestMD)
	reconfigSync := types.ReconfigSync{InReplicatedDecisions: false}
	for _, record := range records {
		proposal := types.Proposal{
			Payload:  record.Batch.toBytes(),
			Metadata: record.Metadata,
		}
		a.logger.Debugf("Sync deliver view %d last seq %d", a.latestMD.ViewId, a.latestMD.LatestSequence)
		a.Deliver(proposal, nil)
		for _, req := range record.Batch.Requests {
			request := requestFromBytes(req)
			if request.Reconfig.InLatestDecision {
				reconfig := request.Reconfig.recconfigToUint(a.ID)
				reconfigSync = types.ReconfigSync{
					InReplicatedDecisions: true,
					CurrentNodes:          reconfig.CurrentNodes,
					CurrentConfig:         reconfig.CurrentConfig,
				}
			}
		}
	}
	return types.SyncResponse{Latest: *a.lastDecision, Reconfig: reconfigSync}
}

// Restart restarts the node
func (a *App) Restart() {
	a.RestartSync(true)
}

func (a *App) RestartSync(sync bool) {
	a.Consensus.Stop()
	a.Node.Lock()
	defer a.Node.Unlock()
	a.Setup()
	a.Consensus.Config.SyncOnStart = sync
	if err := a.Consensus.Start(); err != nil {
		a.logger.Panicf("Consensus start returned an error : %v", err)
	}
}

func (a *App) DelaySync(c <-chan struct{}) {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.syncDelay = c
}

// Disconnect disconnects the node from the network
func (a *App) Disconnect() {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.lossProbability = 1
}

// DisconnectFrom disconnects the node from a specific node
func (a *App) DisconnectFrom(target uint64) {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.peerLossProbability[target] = 1.0
}

// ConnectTo connects the node to a specific node
func (a *App) ConnectTo(target uint64) {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	delete(a.Node.peerLossProbability, target)
}

// Connect connects the node to the network
func (a *App) Connect() {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.lossProbability = 0
}

// MutateSend set the mutating function to be called before sending a message to the target node
func (a *App) MutateSend(target uint64, mutating func(uint64, *smartbftprotos.Message)) {
	a.Node.mutatingFuncLock.Lock()
	defer a.Node.mutatingFuncLock.Unlock()
	a.Node.peerMutatingFunc[target] = mutating
}

// ClearMutateSend clears any mutating function called before sending a message to the target node
func (a *App) ClearMutateSend(target uint64) {
	a.Node.mutatingFuncLock.Lock()
	defer a.Node.mutatingFuncLock.Unlock()
	delete(a.Node.peerMutatingFunc, target)
}

func (a *App) LoseMessages(filter func(*smartbftprotos.Message) bool) {
	a.messageLost = filter
}

// RequestID returns info about the given request
func (a *App) RequestID(req []byte) types.RequestInfo {
	txn := requestFromBytes(req)
	return types.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

// VerifyProposal verifies the given proposal and returns the included requests
func (a *App) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	blockData := batchFromBytes(proposal.Payload)
	requests := make([]types.RequestInfo, 0)
	for _, t := range blockData.Requests {
		req := requestFromBytes(t)
		reqInfo := types.RequestInfo{ID: req.ID, ClientID: req.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

// RequestsFromProposal returns from the given proposal the included requests' info
func (a *App) RequestsFromProposal(proposal types.Proposal) []types.RequestInfo {
	blockData := batchFromBytes(proposal.Payload)
	requests := make([]types.RequestInfo, 0)
	for _, t := range blockData.Requests {
		req := requestFromBytes(t)
		reqInfo := types.RequestInfo{ID: req.ID, ClientID: req.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests
}

// VerifyRequest verifies the given request and returns its info
func (a *App) VerifyRequest(val []byte) (types.RequestInfo, error) {
	req := requestFromBytes(val)
	return types.RequestInfo{ID: req.ID, ClientID: req.ClientID}, nil
}

// VerifyConsenterSig verifies a nodes signature on the given proposal
func (a *App) VerifyConsenterSig(signature types.Signature, _ types.Proposal) ([]byte, error) {
	return signature.Msg, nil
}

func (a *App) AuxiliaryData(msg []byte) []byte {
	return msg
}

// VerifySignature verifies a signature
func (a *App) VerifySignature(_ types.Signature) error {
	return nil
}

// VerificationSequence returns the current verification sequence
func (a *App) VerificationSequence() uint64 {
	return atomic.LoadUint64(&a.verificationSeq)
}

// Sign signs on the given value
func (a *App) Sign([]byte) []byte {
	return nil
}

// SignProposal signs on the given proposal
func (a *App) SignProposal(_ types.Proposal, aux []byte) *types.Signature {
	cnt := a.Node.n.Count()
	if len(aux) == 0 && cnt > 1 && a.messageLost == nil {
		a.logger.Panicf("didn't receive prepares from anyone, n=%d", cnt)
	}
	return &types.Signature{ID: a.ID, Msg: aux}
}

// AssembleProposal assembles a new proposal from the given requests
func (a *App) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	return types.Proposal{
		VerificationSequence: int64(atomic.LoadUint64(&a.verificationSeq)),
		Payload:              batch{Requests: requests}.toBytes(),
		Metadata:             metadata,
	}
}

func (a *App) MembershipChange() bool {
	return false
}

// Deliver delivers the given proposal
func (a *App) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	a.lock.Lock()
	defer a.lock.Unlock()
	defer func() {
		a.lastRecord = lastRecord{
			proposal:   proposal,
			signatures: signatures,
		}
	}()
	record := &AppRecord{
		Metadata: proposal.Metadata,
		Batch:    batchFromBytes(proposal.Payload),
	}
	a.Node.cb.add(record)
	a.lastDecision = &types.Decision{
		Proposal:   proposal,
		Signatures: signatures,
	}

	prevSeq := a.latestMD.LatestSequence

	a.latestMD = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, a.latestMD); err != nil {
		panic(err)
	}

	a.logger.Debugf("Deliver view %d last seq %d prevSeq %d", a.latestMD.ViewId, a.latestMD.LatestSequence, prevSeq)
	if prevSeq == a.latestMD.LatestSequence {
		a.logger.Panicf("Committed sequence %d twice", prevSeq)
	}

	a.Delivered <- record

	for _, req := range record.Batch.Requests {
		request := requestFromBytes(req)
		if request.Reconfig.InLatestDecision {
			reconfig := request.Reconfig.recconfigToUint(a.ID)
			return types.Reconfig{InLatestDecision: true, CurrentNodes: reconfig.CurrentNodes, CurrentConfig: reconfig.CurrentConfig}
		}
	}

	return types.Reconfig{InLatestDecision: false}
}

type committedBatches struct {
	lock     sync.RWMutex
	latestMD *smartbftprotos.ViewMetadata
	records  []*AppRecord
}

func (cb *committedBatches) add(record *AppRecord) {
	cb.lock.Lock()
	defer cb.lock.Unlock()

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(record.Metadata, md); err != nil {
		panic(err)
	}

	if cb.latestMD != nil && cb.latestMD.ViewId > md.ViewId {
		return
	}
	if cb.latestMD != nil && cb.latestMD.LatestSequence >= md.LatestSequence {
		return
	}
	cb.latestMD = md
	cb.records = append(cb.records, record)
}

func (cb *committedBatches) readAll(from *smartbftprotos.ViewMetadata) []*AppRecord {
	cb.lock.RLock()
	defer cb.lock.RUnlock()

	var res []*AppRecord
	for _, entry := range cb.records {
		md := &smartbftprotos.ViewMetadata{}
		if err := proto.Unmarshal(entry.Metadata, md); err != nil {
			panic(err)
		}
		if md.ViewId < from.ViewId || md.LatestSequence <= from.LatestSequence {
			continue
		}
		res = append(res, &AppRecord{
			Metadata: entry.Metadata,
			Batch:    entry.Batch,
		})
	}
	return res
}

// Request represents a client's request
type Request struct {
	ClientID string
	ID       string
	Reconfig Reconfig
}

// ToBytes returns a byte array representation of the request
func (txn Request) ToBytes() []byte {
	rawTxn, err := asn1.Marshal(txn)
	if err != nil {
		panic(err)
	}
	return rawTxn
}

func requestFromBytes(req []byte) *Request {
	var r Request
	asn1.Unmarshal(req, &r)
	return &r
}

type batch struct {
	Requests [][]byte
}

func (b batch) toBytes() []byte {
	rawBlock, err := asn1.Marshal(b)
	if err != nil {
		panic(err)
	}
	return rawBlock
}

func batchFromBytes(rawBlock []byte) *batch {
	var block batch
	asn1.Unmarshal(rawBlock, &block)
	return &block
}

// AppRecord represents a committed batch and metadata
type AppRecord struct {
	Batch    *batch
	Metadata []byte
}

func newNode(id uint64, network *Network, testName string, testDir string, rotateLeader bool, decisionsPerLeader uint64) *App {
	logConfig := zap.NewDevelopmentConfig()
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", testName)).With(zap.Int64("id", int64(id)))
	sugaredLogger := logger.Sugar()

	app := &App{
		clock:           time.NewTicker(time.Second),
		secondClock:     time.NewTicker(time.Second),
		ID:              id,
		Delivered:       make(chan *AppRecord, 100),
		logLevel:        logConfig.Level,
		latestMD:        &smartbftprotos.ViewMetadata{},
		lastDecision:    &types.Decision{},
		logger:          sugaredLogger,
		metricsProvider: &disabled.Provider{},
	}

	config := fastConfig
	config.SelfID = id
	config.SyncOnStart = true
	config.LeaderRotation = rotateLeader
	config.DecisionsPerLeader = decisionsPerLeader

	app.Setup = func() {
		writeAheadLog, walInitialEntries, err := wal.InitializeAndReadAll(
			app.logger,
			filepath.Join(testDir, fmt.Sprintf("node%d", id)),
			&wal.Options{Metrics: wal.NewMetrics(app.metricsProvider)},
		)
		if err != nil {
			sugaredLogger.Panicf("Failed to initialize WAL: %s", err)
		}

		if app.Consensus != nil && app.Consensus.Config.DecisionsPerLeader > 0 {
			config.DecisionsPerLeader = app.Consensus.Config.DecisionsPerLeader
		}
		if app.Consensus != nil && app.Consensus.Config.LeaderRotation {
			config.LeaderRotation = true
		}

		c := &consensus.Consensus{
			Config:             config,
			ViewChangerTicker:  app.secondClock.C,
			Scheduler:          app.clock.C,
			Logger:             app.logger,
			Metrics:            api.NewMetrics(app.metricsProvider),
			WAL:                writeAheadLog,
			Metadata:           app.latestMD,
			Verifier:           app,
			Signer:             app,
			MembershipNotifier: app,
			RequestInspector:   app,
			Assembler:          app,
			Synchronizer:       app,
			Application:        app,
			WALInitialContent:  walInitialEntries,
			LastProposal:       app.lastRecord.proposal,
			LastSignatures:     app.lastRecord.signatures,
		}
		if app.heartbeatTime != nil {
			app.clock.Stop()
			c.Scheduler = app.heartbeatTime
		}
		if app.viewChangeTime != nil {
			app.secondClock.Stop()
			c.ViewChangerTicker = app.viewChangeTime
		}
		network.AddOrUpdateNode(id, c, app)
		c.Comm = network.GetByID(id)
		app.Consensus = c
	}
	app.Setup()
	app.Node = network.GetByID(id)
	return app
}

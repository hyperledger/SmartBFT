// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type nodeServer struct {
	node *bftNode
	mux  *http.ServeMux
}

func newNodeServer(node *bftNode) *nodeServer {
	server := &nodeServer{node: node, mux: http.NewServeMux()}
	server.mux.HandleFunc("/v1/health", server.health)
	server.mux.HandleFunc("/v1/status", server.status)
	server.mux.HandleFunc("/v1/requests", server.submit)
	server.mux.HandleFunc("/v1/trace", server.trace)
	server.mux.HandleFunc("/internal/v1/consensus", server.consensusMessage)
	server.mux.HandleFunc("/internal/v1/transaction", server.forwardedRequest)
	server.mux.HandleFunc("/internal/v1/decisions", server.decisions)
	return server
}

func (s *nodeServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Cache-Control", "no-store")
	s.mux.ServeHTTP(writer, request)
}

func (s *nodeServer) health(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	code := http.StatusServiceUnavailable
	if s.node.started.Load() {
		code = http.StatusOK
	}
	writeJSON(writer, code, map[string]any{"status": map[bool]string{true: "ok", false: "starting"}[s.node.started.Load()], "node_id": s.node.id})
}

func (s *nodeServer) status(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	state, err := s.node.core.state()
	if err != nil {
		writeError(writer, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"status":                      map[bool]string{true: "ok", false: "recovering"}[s.node.started.Load()],
		"application_command_version": 1,
		"node_id":                     s.node.id,
		"leader_id":                   s.node.consensus.GetLeaderID(),
		"membership_version":          s.node.membership.Version,
		"network_id":                  s.node.membership.NetworkID,
		"peer_count":                  len(s.node.membership.Nodes),
		"ledger_height":               state.Height,
		"ledger_head":                 state.LedgerHead,
		"process_model":               "one independent SmartBFT consensus-engine process",
	})
}

func (s *nodeServer) submit(writer http.ResponseWriter, request *http.Request) {
	if !s.node.started.Load() {
		writeError(writer, 503, "engine recovering")
		return
	}
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	raw, err := readBody(writer, request, 64<<10)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := s.node.VerifyRequest(raw); err != nil {
		writeError(writer, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := s.node.consensus.SubmitRequest(raw); err != nil {
		writeError(writer, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"state": "pending", "request_id": requestID(raw), "node_id": s.node.id})
}

func (s *nodeServer) trace(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	after, err := queryUint(request, "after", 0, 0, ^uint64(0))
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := queryUint(request, "limit", 500, 1, 2000)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"events": s.node.trace.list(after, int(limit))})
}

func (s *nodeServer) consensusMessage(writer http.ResponseWriter, request *http.Request) {
	raw, sender, ok := s.authenticatedPeerBody(writer, request, 2<<20)
	if !ok {
		return
	}
	message := &smartbftprotos.Message{}
	if err := proto.Unmarshal(raw, message); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid SmartBFT message")
		return
	}
	kind, view, sequence, digest := consensusMessageDetails(message)
	s.node.trace.add(traceEvent{NodeID: s.node.id, Direction: "receive", Sender: sender, Receiver: s.node.id, MessageType: kind, View: view, Sequence: sequence, ProposalDigest: digest, SizeBytes: len(raw), Outcome: "authenticated_http"})
	s.node.consensus.HandleMessage(sender, message)
	writer.WriteHeader(http.StatusNoContent)
}

func (s *nodeServer) forwardedRequest(writer http.ResponseWriter, request *http.Request) {
	raw, sender, ok := s.authenticatedPeerBody(writer, request, 64<<10)
	if !ok {
		return
	}
	s.node.trace.add(traceEvent{NodeID: s.node.id, Direction: "receive", Sender: sender, Receiver: s.node.id, MessageType: "FORWARDED-REQUEST", SizeBytes: len(raw), Outcome: "authenticated_http"})
	s.node.consensus.HandleRequest(sender, raw)
	writer.WriteHeader(http.StatusNoContent)
}

func (s *nodeServer) authenticatedPeerBody(writer http.ResponseWriter, request *http.Request, maximum int64) ([]byte, uint64, bool) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return nil, 0, false
	}
	raw, err := readBody(writer, request, maximum)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return nil, 0, false
	}
	if request.Header.Get("X-MWVN-Network") != s.node.membership.NetworkID {
		writeError(writer, http.StatusUnauthorized, "network identity mismatch")
		return nil, 0, false
	}
	sender, err := strconv.ParseUint(request.Header.Get("X-MWVN-Sender"), 10, 64)
	if err != nil {
		writeError(writer, http.StatusUnauthorized, "invalid peer identity")
		return nil, 0, false
	}
	publicKey, exists := s.node.publicKeys[sender]
	if !exists {
		writeError(writer, http.StatusUnauthorized, "unknown peer")
		return nil, 0, false
	}
	signature, err := base64.StdEncoding.DecodeString(request.Header.Get("X-MWVN-Signature"))
	if err != nil || !ed25519.Verify(publicKey, transportSigningPayload(s.node.membership.NetworkID, request.URL.Path, raw), signature) {
		writeError(writer, http.StatusUnauthorized, "invalid peer signature")
		return nil, 0, false
	}
	return raw, sender, true
}

func readBody(writer http.ResponseWriter, request *http.Request, maximum int64) ([]byte, error) {
	request.Body = http.MaxBytesReader(writer, request.Body, maximum)
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, errors.New("request body is too large")
	}
	if len(raw) == 0 {
		return nil, errors.New("request body is empty")
	}
	return raw, nil
}

func queryUint(request *http.Request, name string, defaultValue, minimum, maximum uint64) (uint64, error) {
	value := request.URL.Query().Get(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return parsed, nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": strings.TrimSpace(message)})
}

func methodNotAllowed(writer http.ResponseWriter, allowed string) {
	writer.Header().Set("Allow", allowed)
	writeError(writer, http.StatusMethodNotAllowed, "method not allowed")
}

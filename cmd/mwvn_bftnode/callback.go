// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type coreClient struct {
	baseURL string
	client  *http.Client
}

type verifyResponse struct {
	RequestIDs []string `json:"request_ids"`
}

type coreState struct {
	NodeID     uint64 `json:"node_id"`
	Height     uint64 `json:"height"`
	LedgerHead string `json:"ledger_head"`
}

func newCoreClient(baseURL string) *coreClient {
	return &coreClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *coreClient) verifyRequest(raw []byte) (string, error) {
	response := verifyResponse{}
	if err := c.post("/internal/v1/requests/verify", raw, &response); err != nil {
		return "", err
	}
	if len(response.RequestIDs) != 1 || response.RequestIDs[0] == "" {
		return "", fmt.Errorf("Python core returned invalid request identity")
	}
	return response.RequestIDs[0], nil
}

func (c *coreClient) verifyProposal(raw []byte) ([]string, error) {
	response := verifyResponse{}
	if err := c.post("/internal/v1/proposals/verify", raw, &response); err != nil {
		return nil, err
	}
	if len(response.RequestIDs) == 0 {
		return nil, fmt.Errorf("Python core returned no request identities")
	}
	for _, id := range response.RequestIDs {
		if id == "" {
			return nil, fmt.Errorf("Python core returned an empty request identity")
		}
	}
	return response.RequestIDs, nil
}

func (c *coreClient) commit(raw []byte) error {
	return c.post("/internal/v1/commits", raw, nil)
}

func (c *coreClient) state() (coreState, error) {
	var result coreState
	request, err := http.NewRequest(http.MethodGet, c.baseURL+"/internal/v1/state", nil)
	if err != nil {
		return result, err
	}
	request.Header.Set("X-MWVN-Internal-Key", os.Getenv("MWVN_INTERNAL_API_KEY"))
	response, err := c.client.Do(request)
	if err != nil {
		return result, fmt.Errorf("call Python core state: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return result, err
	}
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("Python core state returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := decodeStrictJSON(body, &result); err != nil {
		return result, fmt.Errorf("decode Python core state: %w", err)
	}
	return result, nil
}

func (c *coreClient) post(path string, raw []byte, target any) error {
	request, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-MWVN-Internal-Key", os.Getenv("MWVN_INTERNAL_API_KEY"))
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("call Python core %s: %w", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Python core %s returned HTTP %d: %s", path, response.StatusCode, strings.TrimSpace(string(body)))
	}
	if target != nil {
		if err := json.Unmarshal(body, target); err != nil {
			return fmt.Errorf("decode Python core %s response: %w", path, err)
		}
	}
	return nil
}

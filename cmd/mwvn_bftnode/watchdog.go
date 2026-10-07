package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// Python scheduling is part of the logical validator's availability. Without
// this optional window-profile watchdog an alive Go leader could heartbeat
// forever while its paired application is unavailable and cannot close windows.
func (c *coreClient) applicationHealthy() error {
	request, err := http.NewRequest(http.MethodGet, c.baseURL+"/internal/v1/runtime", nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-MWVN-Internal-Key", os.Getenv("MWVN_INTERNAL_API_KEY"))
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	var state struct {
		WindowMode       bool `json:"window_mode"`
		SchedulerHealthy bool `json:"scheduler_healthy"`
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("application runtime HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		return err
	}
	if !state.WindowMode || !state.SchedulerHealthy {
		return fmt.Errorf("application window scheduler is not healthy")
	}
	return nil
}

func (n *bftNode) watchApplication(stop <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := n.core.applicationHealthy(); err != nil {
				failures++
				n.logger.Warnf("paired application watchdog (%d/3): %v", failures, err)
				if failures >= 3 {
					// Fail-stop, retain WAL/archive, and let the supervisor restart the pair.
					// Peers use unchanged SmartBFT leader-failure detection.
					n.logger.Panicf("paired application unavailable; fail-stopping consensus engine")
				}
			} else {
				failures = 0
			}
		}
	}
}

// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func main() {
	nodeID := flag.Uint64("id", 0, "validator ID from membership")
	listenAddress := flag.String("listen", ":8200", "local and peer HTTP listen address")
	membershipPath := flag.String("membership", "", "shared membership JSON file")
	privateKeyPath := flag.String("private-key", "", "base64 Ed25519 seed file for this node")
	coreURL := flag.String("core-url", "", "paired Python validator callback URL")
	dataDirectory := flag.String("data-dir", "/data", "SmartBFT WAL directory")
	applicationWatchdog := flag.Bool("application-watchdog", false, "fail-stop when the paired evidence-window application is unhealthy")
	flag.Parse()

	if *nodeID == 0 || *membershipPath == "" || *privateKeyPath == "" || *coreURL == "" {
		flag.Usage()
		os.Exit(2)
	}
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync()
	sugar := logger.With(zap.Uint64("node_id", *nodeID)).Sugar()

	config, publicKeys, err := loadMembership(*membershipPath, *nodeID)
	if err != nil {
		sugar.Fatalf("load membership: %v", err)
	}
	privateKey, err := loadPrivateKey(*privateKeyPath, publicKeys[*nodeID])
	if err != nil {
		sugar.Fatalf("load private key: %v", err)
	}
	node, err := newBFTNode(*nodeID, config, publicKeys, privateKey, *coreURL, *dataDirectory, sugar)
	if err != nil {
		sugar.Fatalf("create consensus engine: %v", err)
	}
	defer node.stop()

	server := &http.Server{Addr: *listenAddress, Handler: newNodeServer(node), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)
	if err := node.start(); err != nil {
		sugar.Fatalf("start consensus engine: %v", err)
	}
	sugar.Infof("MWVN SmartBFT engine ready: id=%d listen=%s network=%s core=%s", *nodeID, *listenAddress, config.NetworkID, *coreURL)
	watchdogStop := make(chan struct{})
	defer close(watchdogStop)
	if *applicationWatchdog {
		go node.watchApplication(watchdogStop)
	}

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM)
	select {
	case signal := <-interrupts:
		sugar.Infof("stopping on signal %s", signal)
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			sugar.Fatalf("HTTP server failed: %v", err)
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		sugar.Errorf("HTTP shutdown failed: %v", err)
	}
	fmt.Println("stopped")
}

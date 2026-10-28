# AGENTS.md

This file provides guidance to AI coding agents working with code in this repository.

SmartBFT is a Go BFT state machine replication library modeled on BFT-SMaRt. Dependencies are vendored.

## Commands

Run CI's checks (`.github/workflows/build.yml`) before pushing. Tool versions are pinned in `tools/go.mod`. After each generation step, CI fails if `git status --porcelain` is not empty.

```sh
go mod tidy && go mod vendor

go -C tools install github.com/bufbuild/buf/cmd/buf google.golang.org/protobuf/cmd/protoc-gen-go
buf generate                                       # protobufs in smartbftprotos/

go -C tools install github.com/vektra/mockery/v3
rm -f internal/bft/mocks/*.go && mockery --config .mockery.yml

go -C tools install github.com/golangci/golangci-lint/v2/cmd/golangci-lint
golangci-lint run ./...                            # must report 0 issues

go test -count 1 -race ./...
go test ./test/ -count=1                           # integration suite, a few minutes
go test ./internal/bft/ -run TestViewChangerBasic -count=1 -v
```

- **Mocks:** generated from the interface list in `.mockery.yml`; there are no `//go:generate` directives. To mock a new interface, add it to that list.
- **Formatting:** `.golangci.yml` enables the gci and gofumpt formatters, so `gofmt` alone is not enough.
- **Commits:** need a DCO sign-off (`git commit -s`).

## Architecture

`pkg/consensus.Consensus` is the entry point. The application fills in its exported fields, which are the dependencies defined in `pkg/api`, and calls `Start()`. It then feeds in messages and requests, and decisions come back through `Application.Deliver`.

`createComponents` and `continueCreateComponents` wire the core components in `internal/bft` (imported as `algorithm`) together. Many of them reference each other.
- **`Controller`** owns the current `View`. It runs the leader's proposing loop, delivers decisions, handles sync, and routes messages to the view, the `ViewChanger` or the `StateCollector`. `Consensus` itself is the controller's `Application`, `FailureDetector` and `Synchronizer`, so that it can intercept deliveries and complaints.
- **`View`** runs pre-prepare, prepare and commit for a single view.
- **`ViewChanger`** handles view changes. It uses `InFlightData` to carry a proposal that may already be committed elsewhere into the next view.
- **`RequestPool`** times out pending requests. On timeout it first forwards the request to the leader, then complains, which leads to a view change.
- **`PersistedState`** writes protocol progress to the WAL and restores a `View` from it after a crash.

**Reconfiguration.** When `Deliver` returns a `types.Reconfig`, `Consensus.reconfig` stops every component, rebuilds them with the new config and node set, prunes the request pool, and restarts from the checkpoint. A node that is removed from the set shuts down.

**Integration tests.** `test/` runs a simulated multi-node cluster in one process. `network.go` routes messages in memory. `test_app.go`'s `App` implements every dependency and can disconnect nodes, mutate or drop messages, restart a node, or delay its sync. Use it for crash, partition, Byzantine and reconfig scenarios.

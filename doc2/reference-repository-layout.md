# Reference: repository layout (MWVN adapter)

This map covers the fork additions and the SmartBFT packages the adapter touches. It omits `vendor/` and third-party APIs.

## Added for MWVN

```text
cmd/mwvn_bftnode/
  main.go           flags, HTTP server lifecycle, startup/shutdown
  config.go         membership JSON, key loading, request identity helpers
  config_test.go    membership and configuration tests
  node.go           SmartBFT interface implementation, WAL, process config
  transport.go      signed HTTP peer transport and trace
  callback.go       HTTP client to the paired Python validator
  server.go         public and peer HTTP routes
  Dockerfile        engine image

doc2/               this Diátaxis set
docs/               earlier consolidated DESIGN and INSTALLATION notes
scripts/install-go.sh
Makefile            test-mwvn-bftnode, test-prepare-timeout
```

## Upstream library (selected)

```text
pkg/api/            embedding interfaces and metrics
pkg/consensus/      public Consensus facade (propagates PREPARE timeout)
pkg/types/          Configuration including PrepareVoteCollectionTimeout
pkg/wal/            write-ahead log
pkg/metrics/        metrics providers
internal/bft/       protocol implementation (view.go PREPARE deadline)
smartbftprotos/     consensus protobuf messages
test/               multi-node protocol tests, including PREPARE-deadline tests
examples/naive_chain/   upstream embedding example (not the MWVN HTTP adapter)
```

## Makefile targets

| Target | Command |
|---|---|
| `test-mwvn-bftnode` | `go test -mod=vendor -count=1 ./cmd/mwvn_bftnode` |
| `test-prepare-timeout` | `go test -mod=vendor -count=1 ./internal/bft ./test -run 'Test.*Prepare'` |

## Not in this repository

- `mwvn-validator` — MWVN rules and application ledger
- `mwvn-regional-deployment` — generated cluster, development keys, Compose
- `mwvn-local-aggregator` — local QC submission into a validator

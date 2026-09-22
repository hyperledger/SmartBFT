# Installation

## Purpose

This repository contains the SmartBFT library and the MWVN Go consensus-engine
adapter, `mwvn_bftnode`. It does not contain the Python MWVN validation logic or
the generated multi-node deployment configuration.

For the simplest complete demonstration, use the separate
`mwvn-regional-deployment` repository. Use the instructions below when working
on or testing the Go engine itself.

## Prerequisites

- Git;
- Go 1.26 or newer, as declared by `go.mod`;
- Make;
- Docker only if building the container image.

On macOS, the included helper installs Go with Homebrew when Go is missing:

```bash
./scripts/install-go.sh
```

The helper does not install Homebrew and does not support Linux. On Linux,
install a compatible Go toolchain using the operating system or official Go
installation method.

## Clone the MWVN branch

```bash
git clone https://github.com/ssais-2025/SmartBFT.git
cd SmartBFT
git switch regional-bft-smartbft
```

The MWVN changes are maintained on `regional-bft-smartbft`, not on the fork's
default `main` branch.

## Build

Dependencies are vendored, so the build does not need to download Go modules:

```bash
go build -mod=vendor -o mwvn-bftnode ./cmd/mwvn_bftnode
```

Optional container build:

```bash
docker build -f cmd/mwvn_bftnode/Dockerfile -t mwvn-bftnode:local .
```

## Test

Run the adapter tests:

```bash
make test-mwvn-bftnode
```

Run the focused five-minute PREPARE-deadline tests:

```bash
make test-prepare-timeout
```

The test target covers both the deadline itself and its interaction with the
other SmartBFT timers. It verifies:

- a real `5m` configuration proceeds immediately when quorum is already
  available;
- delayed PREPARE votes reach quorum before the deadline without changing
  view;
- a missing quorum expires first and then uses normal view-change recovery;
- request auto-removal is postponed for the exact active proposal;
- a disconnected leader is still detected by the independent heartbeat timer.

The long-window success test does not sleep for five minutes. Failure and
leader-loss cases use proportionally shorter deadlines so the test suite stays
fast. To include the race detector for the coordinated state:

```bash
go test -race -mod=vendor -count=1 ./internal/bft \
  -run 'Test(PrepareQuorumContinuesWithoutWaitingForTimeout|PrepareVoteCollectionTimeoutComplainsAndAborts|PrepareVoteCollectionStopsWhenViewIsAborted|PrepareBufferStateIsBoundToExactProposalAndRequest|ReqPoolTimeout)$'

go test -race -mod=vendor -count=1 ./test \
  -run 'Test(FourNodePrepare|StrictPrepareBuffer)'
```

## Run a complete generated cluster

Do not start `mwvn_bftnode` by itself for the normal demonstration. Every engine
requires:

- a shared membership file containing at least four validators;
- its own Ed25519 private key;
- its own data directory;
- enough reachable peer engines to satisfy the calculated quorum;
- one reachable paired Python MWVN Regional Validator.

The deployment repository supplies these dependencies:

```bash
git clone --recurse-submodules https://github.com/ssais-2025/mwvn-regional-deployment.git
cd mwvn-regional-deployment
make configure N=4
make up
make test
make status
```

Open the validator API at <http://127.0.0.1:8090/docs>. The public test flow goes
through the Python validator; clients should not submit unvalidated QCs directly
to the Go engine.

Stop the demonstration with:

```bash
make down
```

## Run one engine manually

Manual startup is intended for engine development after a paired Python
validator and the other engines are available:

```bash
./mwvn-bftnode \
  -id 1 \
  -listen :8201 \
  -membership /absolute/path/to/membership.json \
  -private-key /absolute/path/to/node-1.key \
  -core-url http://127.0.0.1:8101 \
  -data-dir /absolute/path/to/node-1-data \
  -prepare-timeout 5m
```

For this `5m` setting, the MWVN adapter automatically configures the request
complaint interval to `5m30s` and the request auto-remove interval to `6m`.
It also enables SmartBFT's opt-in `RequestTimerGuardDuringPrepare`; generic
SmartBFT applications keep their historical behavior unless they explicitly
enable that setting. Heartbeat failure detection remains at its independent
configured value, so a dead leader is not protected by the PREPARE buffer.

The engine stops if the paired validator is unavailable, reports a different
node ID, or already has a non-empty application ledger. Restart and application
state transfer are not implemented in this experimental adapter.

## Verify the engine

After startup:

```bash
curl -sS http://127.0.0.1:8201/v1/health
curl -sS http://127.0.0.1:8201/v1/status
curl -sS 'http://127.0.0.1:8201/v1/trace?after=0&limit=100'
```

See [DESIGN.md](DESIGN.md) for architecture, configuration semantics, the HTTP
API, security limitations, and the file structure.

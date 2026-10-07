# Test the MWVN SmartBFT engine

This guide contains local builds, automated tests, controlled fixtures, and
manual adapter-development procedures. For the supported multi-node runtime,
use [INSTALLATION.md](INSTALLATION.md).

## Test prerequisites

- Go 1.26 or newer
- Make
- A supported C toolchain only when running the Go race detector

Dependencies are committed in `vendor/`. Docker and running Python validators
are not required for the local Go test suite.

## Build the test binary

From the repository root:

```bash
go build -mod=vendor -o mwvn-bftnode ./cmd/mwvn_bftnode
```

The resulting binary is a development artifact and is ignored by Git. The
regional deployment builds its own Docker image from the selected source
checkout.

## Run the tests

Run the complete Go suite:

```bash
make test
```

This runs every Go package with `-count=1`, including the upstream consensus
tests, MWVN adapter, WAL, metrics, and examples. A failure returns a nonzero
status.

## Focused test commands

| Command | Scope |
|---|---|
| `make test-consensus` | `internal/bft` and `test`: PREPARE/COMMIT, leader failure, view change, quorum, partitions, malicious proposals, recovery, and reconfiguration |
| `make test-mwvn-bftnode` | `cmd/mwvn_bftnode`: configuration, peer authentication, Python callbacks, proof validation, persistence, and certified recovery |
| `go test -mod=vendor -count=1 -race ./cmd/mwvn_bftnode` | Adapter tests with the Go race detector |
| `go test -mod=vendor -count=1 -v ./cmd/mwvn_bftnode` | Adapter test names and detailed output |
| `go test -mod=vendor -count=1 -v ./PACKAGE -run TEST_NAME` | One named test while investigating a failure |

The adapter fixtures cover unknown peers, foreign networks, forged signatures,
tampered bodies, path substitution, malformed protobufs, proof quorum for
N=4/5/6/7, duplicate or unknown signers, archive gaps, and acknowledgement
loss. They do not start a complete regional deployment.

The upstream suite includes simulated partitions and Byzantine leader
proposals. Some upstream scenarios may be explicitly skipped. Read the current
test output: a skipped test is not a passed scenario, and parent tests plus
subtests must not be reported as unique scenarios without checking their names.

## Run one engine manually for development

Manual startup is only for adapter development or troubleshooting an existing
cluster. Starting one process alone does not create consensus.

Before starting it, provide:

- at least three other reachable SmartBFT engines;
- one reachable paired Python validator;
- the cluster's generated `membership.json`;
- this node's base64 Ed25519 seed file;
- a private persistent data directory; and
- the internal callback key configured on the paired validator.

```bash
export MWVN_INTERNAL_API_KEY='the-key-configured-on-validator-1'

./mwvn-bftnode \
  -id 1 \
  -listen :8201 \
  -membership /absolute/path/to/membership.json \
  -private-key /absolute/path/to/node-1.seed \
  -core-url http://127.0.0.1:8101 \
  -data-dir /absolute/path/to/node-1-data \
  -application-watchdog
```

| Argument | Development meaning |
|---|---|
| `-id` | Node ID shared by membership and the paired validator. |
| `-listen` | Local health, request, trace, and authenticated peer HTTP address. |
| `-membership` | Shared network ID, peer URLs, node IDs, and Ed25519 public keys. |
| `-private-key` | This node's base64 Ed25519 seed matching its membership public key. |
| `-core-url` | Paired Python validator callback URL. |
| `-data-dir` | Private persistent directory containing `wal/` and `decisions/`. |
| `-application-watchdog` | Stop after three failed paired-validator runtime checks. |

Check the development process:

```bash
curl -sS http://127.0.0.1:8201/v1/health
curl -sS http://127.0.0.1:8201/v1/status
curl -sS 'http://127.0.0.1:8201/v1/trace?after=0&limit=100'
```

A `202` response from `/v1/requests` means only that the request entered the
pool. Read final state from the paired Python validator.

## Regional acceptance tests

The five-minute evidence policy belongs to the Python validator; the engine
orders `OPEN_WINDOW`, `RECORD_QC`, and `CLOSE_WINDOW` as ordinary application
commands. There is no custom five-minute SmartBFT PREPARE timer.

The cross-repository acceptance suite lives in
[mwvn-regional-deployment](https://github.com/ssais-2025/mwvn-regional-deployment).
It starts real Validator/engine pairs for N=4 and N=7 and checks deadline
closure, leader and application failure, quorum loss, restart, catch-up, and
ledger agreement.

Install Go and the Validator development environment, then run from the
deployment repository:

```bash
make -C components/mwvn-validator install
make test
```

For a shorter development profile:

```bash
make test TEST_ARGS=--quick
```

The runner retains process logs, state, ledgers, and `summary.json` under its
`test-results/` directory. See the deployment
[TESTS.md](https://github.com/ssais-2025/mwvn-regional-deployment/blob/main/TESTS.md)
for profile and report details.

## Interpreting results

Record the Go version, source revision, uncommitted changes, exact command,
passes, failures, and skips when reporting a run. Do not weaken quorum or
validation rules to make a test pass.

Decision proofs in this implementation are lists of individual Ed25519
signatures. BLS aggregation is not implemented. These tests do not establish
production transport security, RF accuracy, vessel authenticity, or sustained
performance.

# MWVN SmartBFT Consensus Engine

This repository contains the Go consensus engine used by the MWVN regional
layer. It is based on Hyperledger Labs SmartBFT v1.0.1 and adds the runnable
`mwvn_bftnode` adapter needed to operate SmartBFT as a service.

The engine orders requests that have already been accepted by an MWVN Regional
Validator. It does not validate witness events, build local quorum certificates,
or decide maritime claim state. Those application rules belong to the separate
`mwvn-validator` project.

## Where this project fits

```text
Synthetic witnesses
        |
        v
Local QC Aggregator
        |
        v
Regional Validator (Python) <-> SmartBFT Engine (this project)
                                      |
                                      v
                             other SmartBFT engines
```

A normal regional deployment runs at least four validator/engine pairs. Each
Python validator checks application rules and owns its authoritative SQLite
ledger. Its paired Go engine participates in consensus and returns committed
decisions with a verifiable decision proof.

The current evidence-window commands (`OPEN_WINDOW`, `RECORD_QC`, and
`CLOSE_WINDOW`) are ordinary application commands. The Python validator owns
the five-minute collection policy; SmartBFT continues to use its normal
consensus timers.

## Run the project

One engine cannot form a useful consensus cluster by itself. Run this project
through the
[MWVN regional deployment](https://github.com/ssais-2025/mwvn-regional-deployment),
which generates membership, keys, callback credentials, Python validators, Go
engines, persistent storage, and Local QC Aggregators.

Use [Installation and operation](docs/INSTALLATION.md) for the regional system.
Use [Testing](docs/TEST.md) for local Go builds, focused tests, the race
detector, and manual adapter development.

## How to work with the repository

| Path | Purpose |
|---|---|
| `cmd/mwvn_bftnode/` | MWVN process, HTTP API, peer transport, Python callbacks, recovery, and tests |
| `internal/bft/` | SmartBFT consensus implementation |
| `pkg/consensus/` and `pkg/types/` | Public consensus facade and configuration types |
| `pkg/wal/` | SmartBFT write-ahead log |
| `test/` | Upstream multi-node consensus tests |
| `docs/DESIGN.md` | Architecture, request flow, API, persistence, recovery, and limitations |
| `docs/INSTALLATION.md` | Regional deployment, generated configuration, operation, and health checks |
| `docs/TEST.md` | Local build, tests, fixtures, manual adapter startup, and regional acceptance testing |

For most MWVN changes, start in `cmd/mwvn_bftnode/`. Change the SmartBFT
protocol packages only when the consensus algorithm itself must change. The
current MWVN evidence-window design does not add protocol messages or alter
SmartBFT quorum rules.

## Ledger and engine storage

The authoritative application ledger lives in each Python validator's SQLite
database. This Go engine does not replace that ledger.

Under its configured `-data-dir`, the engine stores:

- `wal/`: SmartBFT protocol state used to resume consensus after restart;
- `decisions/`: exact committed decisions and their quorum signatures, used
  for recovery, verification, and idempotent delivery to the validator.

On commit, the engine saves the certified decision and calls the paired
validator's `POST /internal/v1/commits` endpoint. The commit is complete from
the application's perspective only after the validator durably appends its
ledger record and returns success.

## Runtime behavior

The public node API provides:

- `POST /v1/requests` to submit a request already admitted by the paired
  validator;
- `GET /v1/health` for process readiness;
- `GET /v1/status` for leader, membership, recovery, and ledger information;
- `GET /v1/trace` for a bounded local protocol trace.

An HTTP `202` from `/v1/requests` means the request is pending. Read final
state from the Python validator API after consensus and durable ledger storage.

Keep the engine data directory across restarts. Each engine also requires its
own Ed25519 key, node ID, callback URL, and a shared membership file containing
at least four nodes.

## Documentation

1. Follow [Installation and operation](docs/INSTALLATION.md) to start and
   operate the complete deployment.
2. Read [Engine design](docs/DESIGN.md) to understand component ownership and
   the consensus request flow.
3. Use [Testing](docs/TEST.md) when changing the adapter or consensus packages.
## Upstream project and license

The underlying library is inspired by
[BFT-SMaRt](https://github.com/bft-smart/library). Background on the original
SmartBFT project is available in the
[Hyperledger Labs SmartBFT wiki](https://github.com/hyperledger-labs/SmartBFT/wiki).

Source files are provided under the Apache License 2.0 in [LICENSE](LICENSE).

# Install and operate the MWVN SmartBFT engine

The engine is intended to run as part of the complete regional system. One
engine cannot form a consensus cluster by itself. Use the
[mwvn-regional-deployment](https://github.com/ssais-2025/mwvn-regional-deployment)
repository to generate configuration and start all Validator-SmartBFT pairs
and Local QC Aggregators together.

For local Go builds, automated tests, controlled fixtures, and manual adapter
startup, use [TEST.md](TEST.md).

## Prerequisites

- Git
- Docker Desktop or Docker Engine with Compose
- Make
- Python 3 for the deployment configurator

The deployment builds this engine in Docker. Go is needed for local development
and testing, but it is not required merely to run the generated Docker system.

## Repository layout

Clone the deployment repository with its Validator and SmartBFT component
checkouts. Keep the Local QC Aggregator beside it:

```text
projects/
├── mwvn-regional-deployment/
│   └── components/
│       ├── mwvn-validator/
│       └── SmartBFT/
└── mwvn-local-aggregator/
```

```bash
git clone --recurse-submodules \
  https://github.com/ssais-2025/mwvn-regional-deployment.git
git clone https://github.com/ssais-2025/mwvn-local-aggregator.git
cd mwvn-regional-deployment
```

If the component repositories already exist elsewhere, pass
`SMARTBFT_SOURCE`, `VALIDATOR_SOURCE`, and `AGGREGATOR_SOURCE` to the deployment
command. The deployment guide documents these source overrides.

## Start the regional system

From `mwvn-regional-deployment`:

```bash
make up N=4 A=1
make status
```

`N` is the number of Validator-SmartBFT pairs, and `A` is the number of Local
QC Aggregators. This example starts four independent Go engines, each paired
with its own Python validator.

The deployment configurator generates:

- one shared membership file containing the network, peer URLs, node IDs, and
  Ed25519 public keys;
- one private Ed25519 seed for each engine;
- one callback key for each engine-validator pair;
- one persistent data volume for each engine;
- Validator, Aggregator, and simulator topology; and
- Docker Compose configuration for the selected cluster size.

Do not create engine membership, keys, or callback credentials manually for
the regional runtime.

## Check the running system

The standard N=4 deployment exposes Engine 1 on port 8201:

```bash
curl -sS http://127.0.0.1:8201/v1/health
curl -sS http://127.0.0.1:8201/v1/status
curl -sS 'http://127.0.0.1:8201/v1/trace?after=0&limit=100'
```

- `/v1/health` reports whether startup and recovery completed.
- `/v1/status` reports node identity, leader, membership, recovery status, and
  the paired validator's ledger height and head.
- `/v1/trace` reports bounded node-local protocol and transport events for
  troubleshooting.

Open the paired Validator 1 API at <http://127.0.0.1:8090/docs>. Final request
state belongs to the validator. An engine response of HTTP `202` means only
that it accepted a request into its pool.

Inspect all services and logs:

```bash
make status
make logs
```

## Stop, restart, or remove the deployment

Restart the existing containers without deleting their storage:

```bash
make restart
```

Remove containers while preserving named volumes:

```bash
make down
```

Delete the regional ledgers, WALs, and certified decision archives only when
that data is intentionally no longer needed:

```bash
make reset
```

## Start the evidence-window profile

Evidence windows are implemented by the Python validator. SmartBFT orders
`OPEN_WINDOW`, `RECORD_QC`, and `CLOSE_WINDOW` as ordinary application
commands and continues using its normal heartbeat and view-change timers.

From `mwvn-regional-deployment`:

```bash
make configure N=4
make windows-up
```

The profile exposes Engine 1 at <http://127.0.0.1:28201/v1/status> and its
paired Validator 1 at <http://127.0.0.1:28101/docs>.

Use `make windows-stop` and `make windows-restart` to stop and restart existing
containers. `make windows-down` removes containers but preserves volumes. Use
`make windows-reset` only to intentionally delete the profile's data.

The deployment's
[evidence-window guide](https://github.com/ssais-2025/mwvn-regional-deployment/blob/main/docs/EVIDENCE_WINDOWS.md)
is authoritative for this profile.

## Persistent storage and ledger ownership

Each engine stores consensus and recovery data under its deployment-managed
data directory:

| Store | Owner | Contents |
|---|---|---|
| Validator SQLite database | Paired Python validator | Authoritative application ledger and current MWVN state |
| Engine `wal/` | This Go process | SmartBFT protocol records needed to resume consensus |
| Engine `decisions/` | This Go process | Exact certified proposals and quorum signatures used for verification, catch-up, and redelivery |

Preserve all three stores together across normal restarts. The Go archive is
not a second application ledger, and deleting it while retaining an advanced
validator ledger requires a reviewed recovery or migration.

## Troubleshooting

| Symptom | Meaning and action |
|---|---|
| Engine health remains unavailable | Check its paired validator, generated membership, private key, callback key, and engine logs. |
| Engine status is `recovering` | The process is restoring its WAL/archive or retrieving missing certified decisions. |
| Submission returns HTTP `202` but remains pending | Check current leader, quorum availability, proposal verification, and validator commit callbacks. |
| Engine exits with the watchdog enabled | Its paired evidence-window validator failed three consecutive runtime checks. |
| Recovery reports a sequence or hash mismatch | Preserve all storage and investigate the validator ledger and certified archive as one pair; do not reset one side. |

See the deployment repository's
[installation guide](https://github.com/ssais-2025/mwvn-regional-deployment/blob/main/docs/INSTALLATION.md)
for cluster sizing, source overrides, simulator connection, and generated-file
details. See [DESIGN.md](DESIGN.md) for message formats, consensus flow,
persistence, and recovery validation.

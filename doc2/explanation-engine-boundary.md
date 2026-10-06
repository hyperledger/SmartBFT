# Why `mwvn_bftnode` exists

MWVN regional consensus is split across repositories on purpose. This note explains the split. Protocol background (PBFT-family quorums, view change) is assumed; see [explanation-smartbft-background.md](explanation-smartbft-background.md).

## Four roles

| Component | Owns |
|---|---|
| Local QC Aggregator | Submitting local QCs to a Regional Validator |
| Python Regional Validator (`mwvn-validator`) | Decode/validate QCs, expiry/replay/policy/state machine, durable application ledger, public status API |
| `mwvn_bftnode` (this repository) | Order already-admitted requests, authenticated SmartBFT messages among engines, verify validator decision signatures, deliver commits to the paired validator |
| Regional deployment (`mwvn-regional-deployment`) | Four validators, four engines, membership, keys, containers, end-to-end tests |

The aggregator and simulator call the Python validator. They do not call a Go engine directly. The Go process is not a complete MWVN validator and does not implement witness-event or local-QC policy.

## Why an adapter process

SmartBFT is a library. An embedding application must supply communication, signing, verification, assembly, and delivery. `mwvn_bftnode` is that embedding:

- `Comm` — protobuf consensus messages and JSON request forwards over HTTP;
- `Signer` / `Verifier` — Ed25519 for protocol objects; application/QC checks delegated to Python;
- `Assembler` — deterministic proposal from admitted requests;
- `Application` — commit callback to Python;
- `WAL` — protocol persistence.

One process runs one `consensus.Consensus` instance. An `N`-validator deployment therefore runs `N` Go processes and `N` Python processes. SmartBFT derives `N` from membership and computes `f` and quorum.

Each validator is configured with **one** paired engine URL. Each engine knows **all** Go peers from membership and **one** Python callback URL (`-core-url`).

## Two stores of durable state

| Store | Where | Content |
|---|---|---|
| SmartBFT WAL | engine `-data-dir` | protocol log |
| Application ledger | Python validator | committed regional records |

The common application record hash excludes a node-local decision-proof subset: two honest nodes may hold different valid quorum signature subsets for the same decision (for `N=4`, different valid 3-of-4 sets).

## Replacement boundary

Keeping MWVN policy in Python lets the PBFT-family engine be replaced later without moving QC validation into the consensus library. Keeping SmartBFT as a library lets this HTTP adapter stay thin: membership, transport, callbacks, and one PREPARE collection deadline.

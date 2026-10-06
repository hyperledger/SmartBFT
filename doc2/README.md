# MWVN SmartBFT engine documentation

This folder documents the Go consensus-engine adapter `mwvn_bftnode` in this repository. It is written for MWVN engineers who pair each engine with a Python Regional Validator.

Existing files under `docs/` remain the earlier consolidated notes. Prefer this folder when you want a Diátaxis split (tutorial, how-to, reference, explanation).

## Start here

| If you want to… | Read |
|---|---|
| Complete one successful lesson with a live cluster | [tutorial-first-successful-round.md](tutorial-first-successful-round.md) |
| Solve a specific operational task | The how-to guides below |
| Look up flags, JSON, or HTTP paths | The reference files below |
| Understand *why* the adapter is shaped this way | The explanation files below |

## Tutorial

- [tutorial-first-successful-round.md](tutorial-first-successful-round.md) — build the engine, start a generated four-node demonstration, and follow one request through a successful round.

## How-to guides

- [how-to-build-and-test.md](how-to-build-and-test.md) — compile the binary or image and run adapter tests.
- [how-to-run-one-engine.md](how-to-run-one-engine.md) — start a single process when membership, keys, peers, and a paired validator already exist.
- [how-to-submit-an-admitted-request.md](how-to-submit-an-admitted-request.md) — submit one validator-admitted request and interpret `202 pending`.
- [how-to-inspect-engine-state.md](how-to-inspect-engine-state.md) — use health, status, and trace while debugging.

## Reference

- [reference-cli-and-config.md](reference-cli-and-config.md) — process flags, membership file, and PREPARE deadline setting.
- [reference-http-api.md](reference-http-api.md) — engine HTTP routes and callback paths.
- [reference-repository-layout.md](reference-repository-layout.md) — directories that matter for this adapter (not `vendor/`).

## Explanation

- [explanation-smartbft-background.md](explanation-smartbft-background.md) — short background: this library is a PBFT-family SMR implementation; paper-level theory is assumed, not restated here.
- [explanation-engine-boundary.md](explanation-engine-boundary.md) — library versus adapter versus Python ledger versus deployment.
- [explanation-consensus-round.md](explanation-consensus-round.md) — how one admitted request becomes a committed regional record.
- [explanation-prepare-deadline.md](explanation-prepare-deadline.md) — the five-minute PREPARE collection deadline.
- [explanation-limitations.md](explanation-limitations.md) — intentional experimental limits.

## Out of scope

- Vendored third-party libraries under `vendor/`
- Python Regional Validator policy and state-machine code (`mwvn-validator`)
- Generated Compose files, development keys, and cluster wiring (`mwvn-regional-deployment`), except as the place those artifacts live
- A restatement of SmartBFT safety and liveness proofs (see [explanation-smartbft-background.md](explanation-smartbft-background.md))

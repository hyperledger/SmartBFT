# Intentional limitations of this adapter

`mwvn_bftnode` is an experimental consensus-engine process. The following gaps are deliberate and should stay visible in operations and design discussions.

## Persistence and restart

- Restart from a **non-empty** SmartBFT WAL is not implemented. The process refuses to start if `<data-dir>/wal` already has entries.
- Restart from a **non-empty** Python application ledger is not implemented. The process refuses to start if `GET /internal/v1/state` reports `height != 0`.
- Application-ledger **state transfer** is not integrated. `SyncOnStart` is `false`. The in-process synchronizer only exposes limited local decision state required by the library.

Use a fresh data directory (and a fresh validator ledger) for demonstrations.

## Authentication and transport

- Python-to-Go (`/v1/*`) and Go-to-Python (`/internal/v1/*` on the validator) HTTP calls are **not** authenticated.
- Peer HTTP is Ed25519-signed over network ID, path, and body, but it is **not** TLS, has **no** replay protection, and is intended for an isolated experiment.
- Keys shipped with the deployment repository are **development-only**.

Production still needs TLS or mTLS, authorization, replay protection, secret management, and a recovery story.

## Application semantics

The engine orders **opaque admitted requests**. It does not:

- validate witness events or local QCs;
- run the MWVN conflict-preserving state machine;
- own the durable regional ledger.

Those belong to `mwvn-validator`.

## Consensus features the adapter turns off or narrows

Relative to `types.DefaultConfig`, the process sets batch max count `1`, disables leader rotation, and disables sync-on-start. Do not generalize MWVN runtime behavior to every SmartBFT embedding.

Upstream protocol capabilities (view change, heartbeats, and so on) still exist in the library; this adapter does not implement a full production recovery story around them. See [explanation-smartbft-background.md](explanation-smartbft-background.md).

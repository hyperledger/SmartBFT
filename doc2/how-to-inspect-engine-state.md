# How to inspect engine state

Use this when a round is slow, stuck, or you need to confirm which replica is leader.

All three routes below are node-local. They do not replace validator status for application commit.

## Health

```bash
curl -sS http://127.0.0.1:8201/v1/health
```

- `200` and `"status":"ok"` — consensus has started.
- `503` and `"status":"starting"` — HTTP is up, consensus is not started yet.

The JSON includes `node_id`.

## Status

```bash
curl -sS http://127.0.0.1:8201/v1/status
```

On success (`200`) the body includes:

- `node_id`, `leader_id`, `network_id`, `membership_version`, `peer_count`
- `ledger_height` and `ledger_head` from the paired validator `GET /internal/v1/state`
- `prepare_vote_timeout_seconds`
- `process_model`

If the paired validator state call fails, the engine returns `502`.

`ledger_height` is the **Python** application ledger as this engine last read it, not a substitute for reading validator APIs in a client.

## Trace

```bash
curl -sS 'http://127.0.0.1:8201/v1/trace?after=0&limit=100'
```

- `after` defaults to `0`; `limit` defaults to `500` and must be in `1..2000`.
- The engine returns a bounded list of node-local protocol trace events (send/receive, message type, view, sequence, size, outcome).

Use trace to see whether PRE-PREPARE, PREPARE, COMMIT, or forwarded requests arrived. It is not a global log and not a ledger.

## What not to do

- Do not treat `/v1/status` `ledger_head` as proof that every validator has the same durable record until each validator has accepted its commit callback.
- Do not call `/internal/v1/consensus` or `/internal/v1/transaction` from operators or the simulator; those routes require peer Ed25519 headers.

Full field list: [reference-http-api.md](reference-http-api.md).

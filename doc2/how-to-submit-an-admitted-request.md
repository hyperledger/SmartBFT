# How to submit an admitted request

Use this when a Python Regional Validator has already admitted a request and you need to understand what `POST /v1/requests` does on the paired engine.

The simulator and Local QC Aggregator must not post unvalidated QCs to the Go engine. The public path is validator ingress, then this engine API.

## Submit

`POST /v1/requests` with `Content-Type: application/json` and a body the paired validator will accept on verify. A minimal admitted-QC identity looks like:

```http
POST /v1/requests
Content-Type: application/json

{
  "schema_version": "mwvn-approved-qc/v2",
  "request_id": "request-001",
  "qc_id": "qc-001",
  "claim_hash": "7a9e667f3b8d94b7c0c5f590a8a2c26d662f8e86d73e631cdd8c39f24f40a9b4"
}
```

The engine verifies the body with the paired validator (`POST /internal/v1/requests/verify`) before calling SmartBFT `SubmitRequest`.

## Interpret the engine response

| HTTP status | Meaning |
|---|---|
| `202` | SmartBFT accepted the request for processing. Body includes `"state":"pending"`, `request_id`, and `node_id`. |
| `422` | Validator verify failed or the body was rejected before submit. |
| `503` | Submit into the consensus request pool failed. |
| `400` | Empty or oversized body (maximum 64 KiB). |

`202` is **not** a commit. Two honest engines can still be collecting PREPARE or COMMIT votes. Read final status from the Python validator API (for example a GET of the request on the validator), not from this `202`.

If this engine is not the current leader, SmartBFT forwards the request to the leader over `POST /internal/v1/transaction`. Do not call that peer route from the validator or the simulator.

## After submit

- Commitment appears as a successful `POST /internal/v1/commits` from **each** engine to **its** paired validator, after a verified decision proof.
- Use [how-to-inspect-engine-state.md](how-to-inspect-engine-state.md) if the request stays pending.
- Protocol background: [explanation-smartbft-background.md](explanation-smartbft-background.md). MWVN hop order: [explanation-consensus-round.md](explanation-consensus-round.md).

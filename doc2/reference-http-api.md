# Reference: HTTP API

Routes are implemented in `cmd/mwvn_bftnode/server.go`. Callbacks the engine **calls** are in `cmd/mwvn_bftnode/callback.go`. Callback **handlers** live in `mwvn-validator`, not in this repository.

All engine responses set `X-Content-Type-Options: nosniff` and `Cache-Control: no-store`. JSON uses `Content-Type: application/json; charset=utf-8`.

## Validator → engine (this process)

### `GET /v1/health`

Readiness of this process.

| Status | Body |
|---|---|
| `200` | `{"status":"ok","node_id":<id>}` after `consensus.Start` |
| `503` | `{"status":"starting","node_id":<id>}` before start |
| `405` | method not allowed |

### `GET /v1/status`

Node-local view of leader, membership, paired ledger snapshot, and PREPARE timeout.

Success `200` object fields:

| Field | Meaning |
|---|---|
| `status` | `"ok"` |
| `node_id` | this engine ID |
| `leader_id` | current SmartBFT leader |
| `membership_version` | membership `version` |
| `network_id` | membership `network_id` |
| `peer_count` | number of membership nodes |
| `ledger_height` | paired validator height |
| `ledger_head` | paired validator head hash |
| `prepare_vote_timeout_seconds` | configured PREPARE collection timeout |
| `process_model` | `"one independent SmartBFT consensus-engine process"` |

| Status | When |
|---|---|
| `200` | validator state call succeeded |
| `502` | `GET {core-url}/internal/v1/state` failed |
| `405` | wrong method |

### `POST /v1/requests`

Submit one request already admitted by the Python validator.

- Maximum body: 64 KiB, non-empty.
- Engine calls `POST {core-url}/internal/v1/requests/verify` first.
- Then `consensus.SubmitRequest`.

Success `202`:

```json
{"state":"pending","request_id":"<id>","node_id":<id>}
```

`request_id` is the JSON `request_id` field when present and non-empty; otherwise the hex SHA-256 of the raw body.

| Status | When |
|---|---|
| `202` | accepted for consensus processing (not committed) |
| `400` | empty or too large body |
| `422` | verify failed |
| `503` | submit failed |
| `405` | wrong method |

### `GET /v1/trace`

Bounded node-local trace.

Query parameters:

| Name | Default | Range |
|---|---|---|
| `after` | `0` | `0` .. max `uint64` |
| `limit` | `500` | `1` .. `2000` |

Success `200`: `{"events":[...]}`. Invalid query: `400`.

## Engine → paired Python validator (this process is the client)

Base URL is `-core-url`. HTTP client timeout is 5 seconds. These paths are implemented by `mwvn-validator`.

| Method | Path | Purpose | Expected success |
|---|---|---|---|
| `GET` | `/internal/v1/state` | Paired `node_id`, application `height`, `ledger_head` | `200` JSON |
| `POST` | `/internal/v1/requests/verify` | Validate one submitted request; return identities | `2xx` JSON `request_ids` of length 1 |
| `POST` | `/internal/v1/proposals/verify` | Validate proposal contents and ledger linkage | `2xx` JSON `request_ids` non-empty |
| `POST` | `/internal/v1/commits` | Deliver decided record and decision proof | `2xx` |

Verify response shape used by the engine:

```json
{"request_ids":["..."]}
```

State shape used by the engine:

```json
{"node_id":1,"height":0,"ledger_head":"..."}
```

Python-to-Go and Go-to-Python HTTP are not authenticated in this experimental adapter.

## Engine → engine (peer)

Both routes require:

| Header | Rule |
|---|---|
| `X-MWVN-Network` | exact membership `network_id` |
| `X-MWVN-Sender` | decimal peer ID present in membership |
| `X-MWVN-Signature` | standard base64 Ed25519 signature over the transport signing payload (protocol domain, network ID, URL path, exact body bytes) |

The simulator and the Python validator must not call these routes.

### `POST /internal/v1/consensus`

- Body: SmartBFT protobuf (`smartbftprotos.Message`), maximum 2 MiB.
- Success: `204` with empty body after `HandleMessage`.
- Invalid protobuf: `400`.
- Auth failure: `401`.

### `POST /internal/v1/transaction`

- Body: JSON application request, maximum 64 KiB.
- Success: `204` after `HandleRequest` (forward to leader).
- Auth failure: `401`.

## Error body

Failed JSON responses use `{"error":"<message>"}`.

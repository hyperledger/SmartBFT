# Reference: CLI flags and configuration

Process flags are defined in `cmd/mwvn_bftnode/main.go`. Membership loading is in `cmd/mwvn_bftnode/config.go`. The PREPARE deadline is a SmartBFT `types.Configuration` field applied in `cmd/mwvn_bftnode/node.go`.

## Command-line flags

| Flag | Type | Default | Required | Meaning |
|---|---|---|---|---|
| `-id` | `uint64` | `0` | yes (must be `> 0`) | Validator ID; must appear in membership. |
| `-listen` | string | `:8200` | no | HTTP listen address for public and peer routes. |
| `-membership` | string | empty | yes | Path to shared membership JSON. |
| `-private-key` | string | empty | yes | Path to this node’s base64 Ed25519 **seed** (32 bytes decoded). |
| `-core-url` | string | empty | yes | Base URL of the paired Python validator (callback origin). |
| `-data-dir` | string | `/data` | no | Directory for the SmartBFT WAL (`<data-dir>/wal`). |
| `-prepare-timeout` | duration | `5m` | yes (must be `> 0`) | Maximum PREPARE vote-collection time. |

Missing required values or `-prepare-timeout <= 0` causes `flag.Usage()` and exit status `2`.

The private key file must decode to an Ed25519 seed whose derived public key matches this node’s `public_key` in membership.

## Membership JSON

Schema version must be exactly `mwvn-smartbft-membership/v1`. Unknown JSON fields are rejected. Multiple JSON values in one file are rejected.

| Field | Type | Constraint |
|---|---|---|
| `schema_version` | string | `mwvn-smartbft-membership/v1` |
| `network_id` | string | non-empty |
| `version` | `uint64` | `> 0` |
| `nodes` | array | length `>= 4` |

Each element of `nodes`:

| Field | Type | Constraint |
|---|---|---|
| `id` | `uint64` | `> 0`, unique |
| `peer_url` | string | HTTP origin only (`http` scheme, host present, **empty path**), unique |
| `public_key` | string | standard base64 Ed25519 public key (32 bytes decoded) |

`-id` must match one `nodes[].id`.

## Engine-local SmartBFT settings

`mwvn_bftnode` starts from `types.DefaultConfig` and then sets:

| Setting | Value in the adapter |
|---|---|
| `SelfID` | `-id` |
| `RequestBatchMaxCount` | `1` |
| `RequestBatchMaxBytes` | `1024 * 1024` |
| `RequestBatchMaxInterval` | `25ms` |
| `RequestMaxBytes` | `64 * 1024` |
| `RequestPoolSize` | `100` |
| `SyncOnStart` | `false` |
| `LeaderRotation` | `false` |
| `DecisionsPerLeader` | `0` |
| `PrepareVoteCollectionTimeout` | `-prepare-timeout` |

`types.DefaultConfig` itself uses a five-minute `PrepareVoteCollectionTimeout`. Library validation rejects a non-positive public timeout.

Default library batching and leader-rotation values are **not** what the MWVN process uses. Do not assume `DefaultConfig` batch size `100` or leader rotation when operating `mwvn_bftnode`.

## PREPARE deadline semantics (facts)

- Field: `PrepareVoteCollectionTimeout` (`time.Duration`).
- Default: `5 * time.Minute`.
- Positive values only in public configuration.
- Applies while collecting matching PREPARE votes after accepting a proposal.
- Quorum ends the wait immediately; expiry complains about the view and aborts it.
- Does not apply to COMMIT collection.

Discussion: [explanation-prepare-deadline.md](explanation-prepare-deadline.md).

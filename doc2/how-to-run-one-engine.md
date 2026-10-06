# How to run one engine by hand

Use this when a paired Python Regional Validator and the other engines already exist, and you need to start or restart a single `mwvn_bftnode` for development. Do not use a lone process as the normal demonstration.

## Requirements

Every engine process needs:

- a shared membership JSON file with at least four nodes;
- this node’s Ed25519 private-key seed file;
- a dedicated data directory for the SmartBFT WAL;
- enough reachable peer engines to form the calculated quorum;
- a reachable paired Python validator at `-core-url`.

Membership, keys, and Compose wiring for the stock demonstration live in `mwvn-regional-deployment`.

## Start the process

```bash
./mwvn-bftnode \
  -id 1 \
  -listen :8201 \
  -membership /absolute/path/to/membership.json \
  -private-key /absolute/path/to/node-1.key \
  -core-url http://127.0.0.1:8101 \
  -data-dir /absolute/path/to/node-1-data \
  -prepare-timeout 5m
```

Adjust `-id`, listen port, paths, and `-core-url` to match membership. Flag dictionary: [reference-cli-and-config.md](reference-cli-and-config.md).

The process exits with status `2` if `-id` is `0` or if `-membership`, `-private-key`, `-core-url`, or a positive `-prepare-timeout` is missing.

## Known stop conditions at startup

The engine refuses to start when any of the following is true:

- the paired validator is unreachable;
- `GET {core-url}/internal/v1/state` reports a `node_id` that does not match `-id`;
- the paired application ledger height is not zero (restart from a non-empty application ledger is not implemented);
- the SmartBFT WAL directory already contains entries (restart from a non-empty WAL is not implemented).

Use a fresh data directory for experimental runs. See [explanation-limitations.md](explanation-limitations.md).

## Confirm it is up

```bash
curl -sS http://127.0.0.1:8201/v1/health
curl -sS http://127.0.0.1:8201/v1/status
```

`/v1/health` returns `503` with `"status":"starting"` until consensus has started, then `200` with `"status":"ok"`.

## Stop

Send `SIGINT` or `SIGTERM`. The process shuts down the HTTP server (five-second timeout) and prints `stopped`.

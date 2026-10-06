# Tutorial: complete one successful engine round

This lesson walks a newcomer from a clone of this repository to a working picture of one regional consensus round. You will build `mwvn_bftnode`, start a generated four-engine demonstration from the separate deployment repository, and inspect one engine. You will not implement Python validation policy.

## What success looks like

By the end you can:

- build the Go engine from this repository;
- start a four-validator, four-engine demonstration using `mwvn-regional-deployment`;
- name the hop order of one request (aggregator → Python validator → paired engine → peer engines → commit callback → validator status);
- obtain `200` from that engine’s `/v1/health` and `/v1/status`.

## Before you start

You need Git, Make, and Go 1.26 or newer (`go.mod` declares `go 1.26.0`). Docker is required only if the deployment repository starts containers. On macOS, if Go is missing:

```bash
./scripts/install-go.sh
```

That helper does not install Homebrew and does not support Linux.

This lesson uses a generated cluster. Do not treat a lone `mwvn_bftnode` process as a complete demonstration: every engine needs shared membership, its own key and data directory, reachable peers for quorum, and a paired Python validator.

## Step 1 — Clone the MWVN branch

```bash
git clone https://github.com/ssais-2025/SmartBFT.git
cd SmartBFT
git switch regional-bft-smartbft
```

MWVN work lives on `regional-bft-smartbft`, not on the fork’s default `main`.

## Step 2 — Build the engine

Dependencies are vendored. From the repository root:

```bash
go build -mod=vendor -o mwvn-bftnode ./cmd/mwvn_bftnode
```

A successful build produces `./mwvn-bftnode`. You will not start this binary yet.

## Step 3 — Start the generated cluster

The deployment repository supplies membership, development keys, Compose wiring, Python validators, and engines:

```bash
git clone --recurse-submodules https://github.com/ssais-2025/mwvn-regional-deployment.git
cd mwvn-regional-deployment
make configure N=4
make up
make test
make status
```

Open the validator API docs at `http://127.0.0.1:8090/docs` (port as published by that deployment). Clients submit local QCs to the Python validator, not unvalidated payloads to the Go engine.

## Step 4 — Follow one successful round

Keep this order in mind. Exact validator URLs belong to `mwvn-validator` and the deployment; the engine’s role is the middle of the chain.

1. A Local QC Aggregator submits a local QC to one Python Regional Validator.
2. That validator authenticates, checks policy, and reserves a durable request identity.
3. The validator posts the admitted request to its paired engine `POST /v1/requests`.
4. The engine asks the same validator to verify the request (`POST /internal/v1/requests/verify`).
5. If this engine is not the leader, it forwards the request to the current leader over the signed peer route.
6. The leader proposes. Every engine asks *its own* paired validator to verify the proposal (`POST /internal/v1/proposals/verify`) before PREPARE voting.
7. Engines exchange PREPARE, then COMMIT, using SmartBFT quorums (for `N=4`, quorum `3`).
8. Each engine delivers the decided record and decision proof to its paired validator (`POST /internal/v1/commits`).
9. Status for the original client is read from the Python validator, not from the engine’s `202` response.

Background for *why* PREPARE and COMMIT quorums decide a sequence: [explanation-smartbft-background.md](explanation-smartbft-background.md). The MWVN-specific hop order: [explanation-consensus-round.md](explanation-consensus-round.md).

## Step 5 — Inspect one engine

After `make up`, query an engine listen address from the generated configuration. A typical local mapping uses ports in the `820x` range; confirm the actual port in the deployment output. Example against a process listening on `127.0.0.1:8201`:

```bash
curl -sS http://127.0.0.1:8201/v1/health
curl -sS http://127.0.0.1:8201/v1/status
curl -sS 'http://127.0.0.1:8201/v1/trace?after=0&limit=100'
```

Expect `status` `ok` and a numeric `node_id` once the process has started consensus. `202` on `/v1/requests` never means the QC is committed.

## Step 6 — Stop the demonstration

From the deployment repository:

```bash
make down
```

## What you learned

`mwvn_bftnode` orders requests the Python validator has already admitted. SmartBFT supplies the replica protocol; this process supplies HTTP, membership, keys, and callbacks. Next: [how-to-build-and-test.md](how-to-build-and-test.md) for a tighter test loop, or [explanation-engine-boundary.md](explanation-engine-boundary.md) for the repository split.

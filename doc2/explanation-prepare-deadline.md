# The PREPARE vote-collection deadline

This fork adds a public duration `PrepareVoteCollectionTimeout` (CLI `-prepare-timeout`, default five minutes). It is a **maximum wait** while a replica collects matching PREPARE votes after accepting a proposal.

## What it is

```text
Enter PREPARE collection
        |
        +-- required matching PREPARE votes arrive before the deadline
        |       -> stop the timer -> send COMMIT immediately
        |
        +-- the deadline expires first
                -> complain about the current view
                -> abort the current view
                -> continue on SmartBFT’s existing view-change path
```

The replica needs `Quorum-1` remote PREPARE votes matching the proposal digest (the replica’s own accept is already implied by being in this phase). Code: `internal/bft/view.go` `processPrepares`.

## What it is not

- Not a mandatory five-minute sleep.
- Not a rolling batching window.
- Not a COMMIT-phase timer.
- Not a change to quorum formulas, signatures, message encodings, heartbeats, or view-change safety rules from upstream SmartBFT.

If the PREPARE quorum is available in milliseconds, the view advances in milliseconds. Tests that prove timeout-triggered abort use short durations so they finish quickly; tests of the “happy path” keep a five-minute configuration and still expect immediate progress at quorum.

## Why MWVN exposes it

Regional MWVN experiments wanted a longer, explicit bound while PREPARE votes arrive, without turning PREPARE into a fixed-length barrier. Expiry is recovery through the library, not a new consensus algorithm. Paper-level justification for view change after a stuck view remains upstream SmartBFT; this adapter only chooses when to complain because PREPARE votes did not arrive in time.

## Configuration path

| Location | Role |
|---|---|
| `pkg/types/config.go` | Field, five-minute default, reject `<= 0` on public config |
| `pkg/consensus/consensus.go` | Copy into internal view parameters |
| `internal/bft/util.go` | Pass into a view |
| `internal/bft/view.go` | Timer in `processPrepares` |
| `cmd/mwvn_bftnode/main.go` | `-prepare-timeout` |
| `cmd/mwvn_bftnode/node.go` | Assign onto `consensus.Config` |

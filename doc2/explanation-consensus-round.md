# How one admitted request becomes a regional decision

This discussion follows a successful round. Quorum and view-change *safety* are SmartBFT’s; this note covers MWVN wiring. Background: [explanation-smartbft-background.md](explanation-smartbft-background.md).

## Ingress is Python, ordering is Go

The Local QC Aggregator posts to a Regional Validator. That validator authenticates, canonicalizes, checks state, and reserves a durable request ID. Only then does it `POST /v1/requests` on its paired engine.

The engine immediately asks the same validator to verify the request (`/internal/v1/requests/verify`) and returns `202 pending` when SmartBFT accepts the bytes into the request pool. Pending means “in the protocol,” not “on every ledger.”

## Leader and forwarding

If the ingress engine is not the current leader, SmartBFT forwards the request to the leader (`POST /internal/v1/transaction` with peer signatures). The leader includes admitted requests in a proposal (`mwvn-smartbft-proposal/v1` payload of approved QCs). The MWVN adapter configures batch max count `1` and disables leader rotation; leadership still changes through the library’s view-change path when the current leader fails.

## Verify the proposal before PREPARE

After a proposal is distributed, **every** engine asks **its own** paired validator to verify the proposal (`/internal/v1/proposals/verify`). Voting is not “trust the leader’s bytes.” Each Python replica checks proposal contents and application-ledger linkage independently.

Only then do replicas collect matching PREPARE votes. With `N=4`, SmartBFT uses quorum `3` (tolerating `f=1`). PREPARE collection is bounded by `-prepare-timeout`; see [explanation-prepare-deadline.md](explanation-prepare-deadline.md). COMMIT collection has no that extra deadline in this fork.

## Delivery is N independent ledger writes

After COMMIT quorum and verification of the decision proof, each engine `POST`s `/internal/v1/commits` to its paired validator. Each Python process appends to **its** ledger and returns HTTP success only after persistence.

Clients should observe committed state on the validator they used for ingress (or another validator that has applied the same decision), never by treating engine `202` as final.

## What the diagrams in older notes compress

Peer PREPARE and COMMIT floods are easy to over-draw. Operationally:

- validators never gossip SmartBFT votes;
- engines never apply MWVN conflict rules;
- a round is finished for a validator when **that** validator has persisted the commit callback, which should match the same consensus-decided record on every honest validator.

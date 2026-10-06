# Background: SmartBFT as a PBFT-family library

This note is **background**, not a tutorial and not a substitute for the SmartBFT academic treatment. MWVN engineers can treat the following as given when they work on `mwvn_bftnode`.

## What this repository inherits

Upstream SmartBFT is a Byzantine fault-tolerant state-machine replication library written in Go. The implementation is inspired by the BFT-SMaRt project. The library, not this adapter, owns:

- the replica protocol (proposal, PREPARE, COMMIT, view change, heartbeats, and related messages);
- quorum calculation from membership size `N` (for example `N=4` yields `f=1` and quorum `3`; `N=7` yields `f=2` and quorum `5`);
- write-ahead logging of protocol state;
- the Go interfaces an embedding application must supply (`Comm`, `Signer`, `Verifier`, `Assembler`, `Application`, and related types).

Paper-level arguments for agreement, validity, and liveness under at most `f` Byzantine replicas belong to that literature and to the upstream Hyperledger Labs SmartBFT wiki. This documentation set does not reproduce those proofs, message-complexity analyses, or formal models.

## What MWVN does not change

The MWVN fork does not invent a new consensus algorithm. It embeds the existing library in a process (`mwvn_bftnode`) and adds one public timing control: a maximum wait while collecting matching PREPARE votes. View-change recovery after that deadline expires is the library’s existing path. See [explanation-prepare-deadline.md](explanation-prepare-deadline.md).

## What you still need from the papers and wiki

When you reason about *why* a quorum of matching PREPARE or COMMIT votes is enough, *when* a view change is safe, or *how* leader failure is tolerated, use the upstream SmartBFT materials. When you reason about *what this Go process accepts over HTTP* and *what the Python validator must persist*, stay in this `doc2` set.

In-repo pointers (not a literature review):

- repository `README.md` — library identity and wiki link;
- `pkg/consensus` and `pkg/api` — the public embedding surface;
- `internal/bft` — protocol implementation (read when contributing, not when integrating the HTTP adapter).

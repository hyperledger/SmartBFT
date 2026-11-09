# MWVN SmartBFT Consensus Engine

This repository contains the Go consensus engine used by the MWVN regional
layer. It is based on Hyperledger Labs SmartBFT v1.0.1 and adds the runnable
`mwvn_bftnode` adapter needed to operate SmartBFT as a service.

The engine orders requests that have already been accepted by an MWVN Regional
Validator. It does not validate witness events, build local quorum certificates,
or decide maritime claim state. Those application rules belong to the separate
`mwvn-validator` project.



```text
Synthetic witnesses
        |
        v
Local QC Aggregator
        |
        v
Regional Validator (Python) <-> SmartBFT Engine (this project)
                                      |
                                      v
                             other SmartBFT engines
```

A normal regional deployment runs at least four validator/engine pairs. Each
Python validator checks application rules and owns its authoritative SQLite
ledger. Its paired Go engine participates in consensus and returns committed
decisions with a verifiable decision proof.

The current evidence-window commands (`OPEN_WINDOW`, `RECORD_QC`, and
`CLOSE_WINDOW`) are ordinary application commands. The Python validator owns
the five-minute collection policy; SmartBFT continues to use its normal
consensus timers.

## Run the project

One engine cannot form a useful consensus cluster by itself. Run this project
through the
[MWVN regional deployment](https://github.com/ssais-2025/mwvn-regional-deployment),
which generates membership, keys, callback credentials, Python validators, Go
engines, persistent storage, and Local QC Aggregators.

# How to build and test `mwvn_bftnode`

Use this when you need a binary, an image, or a focused test run in this repository. For a full four-node demonstration, use the deployment repository (see the tutorial).

## Prerequisites

- Git
- Go 1.26 or newer, as in `go.mod`
- Make, for the test targets
- Docker, only for the container build

On macOS, if Go is missing:

```bash
./scripts/install-go.sh
```

Install Go yourself on Linux. The helper does not install Homebrew.

## Build the binary

From the repository root. Modules are vendored; do not require a module download:

```bash
go build -mod=vendor -o mwvn-bftnode ./cmd/mwvn_bftnode
```

## Build the container image

```bash
docker build -f cmd/mwvn_bftnode/Dockerfile -t mwvn-bftnode:local .
```

## Run adapter tests

Membership and configuration tests for the process:

```bash
make test-mwvn-bftnode
```

This runs `go test -mod=vendor -count=1 ./cmd/mwvn_bftnode`.

## Run PREPARE-deadline tests

```bash
make test-prepare-timeout
```

This runs `go test -mod=vendor -count=1 ./internal/bft ./test -run 'Test.*Prepare'`.

Those tests use short deadlines so failure and recovery finish quickly. A normal cluster configuration still uses the five-minute maximum and proceeds as soon as the PREPARE quorum arrives; it does not wait the full five minutes. See [explanation-prepare-deadline.md](explanation-prepare-deadline.md).

## Related

- Flags after you have a binary: [reference-cli-and-config.md](reference-cli-and-config.md)
- Starting one process by hand: [how-to-run-one-engine.md](how-to-run-one-engine.md)

.PHONY: test test-mwvn-bftnode test-consensus
test:
	go test -mod=vendor -count=1 ./...
test-mwvn-bftnode:
	go test -mod=vendor -count=1 ./cmd/mwvn_bftnode
test-consensus:
	go test -mod=vendor -count=1 ./internal/bft ./test

.PHONY: test test-integration vet gen gen-check
# buf runs in a throwaway container (nothing installed locally); the proto files come from the pinned be-protocol.
BUF := bufbuild/buf:1.74.0@sha256:621f108266b19236906aa328e64b7c50340738472c5da4d4b25104f01aa8d204
PROTO = $(shell go list -m -f '{{.Dir}}' github.com/brickKit/be-protocol/v3)/proto
RUN = docker run --rm -u $(shell id -u):$(shell id -g) -e HOME=/tmp -e BUF_CACHE_DIR=/tmp/buf -v $(CURDIR):/w -v $(PROTO):/proto:ro -w /w
gen:  ## internal/sourcepb and sessionpb from be-protocol's proto/ (committed)
	rm -rf internal/sourcepb sessionpb
	$(RUN) $(BUF) generate /proto --template buf.gen.yaml
gen-check:  ## the committed code is exactly what buf generates from the pinned proto/
	@T=$$(mktemp -d) && cp -r internal/sourcepb sessionpb $$T/ && $(MAKE) -s gen >/dev/null && \
	  diff -r $$T/sourcepb internal/sourcepb && diff -r $$T/sessionpb sessionpb && rm -rf $$T && echo "generated code up to date"
vet:  ## go vet with and without the integration tag, and gofmt (unformatted files fail)
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "gofmt: the files above are not formatted"; exit 1; }
	go vet ./...
	go vet -tags integration ./...
test: vet  ## L1: unit tests, protocol vectors, the SDK's own merge-safety check
	go test -count=1 ./...
# Needs BE_SDK_TEST_PG_ADMIN, a PostgreSQL 16+ superuser DSN (the assembly project's be-postgres), and
# BE_SDK_TEST_NATS_URL, a JetStream-enabled NATS server (be-nats); fails without them.
# -p 1: the packages share one test database, its roles and database-level grants.
test-integration: vet
	go test -count=1 -p 1 -tags integration ./...

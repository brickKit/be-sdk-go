# be-sdk-go: the official Go implementation of be-protocol 1.0. Not a brickKit component.
#
#   make test               unit tests and vectors (no containers; integration tests skip)
#   make test-integration   the same plus every *_integration_test.go against throwaway containers
#   make vectors            only the be-protocol vector suites (pinned module github.com/brickKit/be-protocol)
#   make lint               go vet + gofmt
#   make import-scan        the SDK depends on no component repository (only be-protocol and family contracts)
#
# test-integration starts PostgreSQL 16, PostgreSQL 14 and NATS 2.12 (prefix $(PREFIX)), runs, and removes them.
.DEFAULT_GOAL := help
.PHONY: help test test-integration vectors lint import-scan containers-up containers-down

PREFIX ?= sdkb-go-ci

help:
	@grep -E '^#   make' $(MAKEFILE_LIST) | sed 's/^#   //'

test:
	go test -race -count=1 ./...

vectors:
	go test -count=1 -run 'Vectors|TestVectors' ./internal/...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

import-scan:
	@bad="$$(go list -deps ./... | grep '^github.com/brickKit/' | grep -vE '^github.com/brickKit/(be-sdk-go|be-protocol|contract-infra-authz/v2)($$|/)')"; \
	if [ -n "$$bad" ]; then echo "be-sdk-go must not depend on: $$bad"; exit 1; fi; echo "no component dependency"

containers-up:
	docker run -d --rm --name $(PREFIX)-pg16 -e POSTGRES_PASSWORD=sdkb --tmpfs /var/lib/postgresql/data -p 127.0.0.1::5432 postgres:16-alpine >/dev/null
	docker run -d --rm --name $(PREFIX)-pg14 -e POSTGRES_PASSWORD=sdkb --tmpfs /var/lib/postgresql/data -p 127.0.0.1::5432 postgres:14-alpine >/dev/null
	docker run -d --rm --name $(PREFIX)-nats -p 127.0.0.1::4222 nats:2.12-alpine -js >/dev/null
	@for c in pg16 pg14; do until docker exec $(PREFIX)-$$c pg_isready -U postgres -q 2>/dev/null; do sleep 1; done; done; sleep 1

containers-down:
	-docker rm -f $(PREFIX)-pg16 $(PREFIX)-pg14 $(PREFIX)-nats >/dev/null 2>&1

test-integration: containers-up
	@set -e; trap '$(MAKE) -s containers-down' EXIT; \
	export TEST_PG16_DSN="postgres://postgres:sdkb@$$(docker port $(PREFIX)-pg16 5432)/postgres?sslmode=disable"; \
	export TEST_PG14_DSN="postgres://postgres:sdkb@$$(docker port $(PREFIX)-pg14 5432)/postgres?sslmode=disable"; \
	export TEST_NATS_URL="nats://$$(docker port $(PREFIX)-nats 4222)"; \
	go test -race -count=1 ./...

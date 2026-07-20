COMPOSE := docker compose

.DEFAULT_GOAL := help

## help: list available targets
.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //'

# ---- full stack (everything in Docker) ----

## up: build images and start the whole system (kafka + api + workers + receiver)
.PHONY: up
up:
	$(COMPOSE) up -d --build

## down: stop everything (keeps Kafka data volume)
.PHONY: down
down:
	$(COMPOSE) down

## reset: stop everything AND wipe Kafka data (topics/messages/offsets)
.PHONY: reset
reset:
	$(COMPOSE) down -v

## logs: follow logs from all services
.PHONY: logs
logs:
	$(COMPOSE) logs -f

## ps: show running services
.PHONY: ps
ps:
	$(COMPOSE) ps

# ---- local iteration (Kafka in Docker, services with `go run`) ----

## infra: start only Kafka + create topics, for running services locally
.PHONY: infra
infra:
	$(COMPOSE) up -d kafka kafka-init

## run-api: run the API locally (needs `make infra`)
.PHONY: run-api
run-api:
	go run ./cmd/api

## run-worker: run a delivery worker locally
.PHONY: run-worker
run-worker:
	go run ./cmd/worker

## run-retry-worker: run the retry worker locally
.PHONY: run-retry-worker
run-retry-worker:
	go run ./cmd/retry-worker

## run-receiver: run the test receiver locally
.PHONY: run-receiver
run-receiver:
	go run ./cmd/webhook-accept-api

## tester: fire load at the local API
.PHONY: tester
tester:
	go run ./cmd/tester

# ---- quality gates ----

## build: compile everything
.PHONY: build
build:
	go build ./...

## test: run tests with the race detector
.PHONY: test
test:
	go test ./... -race

## vet: run go vet
.PHONY: vet
vet:
	go vet ./...

## lint: run golangci-lint (requires it to be installed)
.PHONY: lint
lint:
	golangci-lint run

## fmt: format all Go code
.PHONY: fmt
fmt:
	gofmt -w .

## tidy: tidy go.mod/go.sum
.PHONY: tidy
tidy:
	go mod tidy

## check: build + vet + test (what CI runs)
.PHONY: check
check: build vet test

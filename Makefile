.PHONY: build lint build-lint test test-unit test-integration coverage coverage-html \
        deps install-tools install-lint-tools install-test-tools clean clean-tools \
        doc-refresh help

## ——— Tooling ——————————————————————————————————————————
# Dev tools are installed into ./bin (gitignored) at pinned versions so that
# local runs and CI use the exact same binaries. golangci-lint's version lives
# in .golangci-lint-version, which .github/workflows/build-lint.yml reads too —
# bump it there and both sides follow.
GOBASE ?= $(CURDIR)
GOBIN  ?= $(GOBASE)/bin

GOLANGCI_VERSION  ?= $(shell cat $(GOBASE)/.golangci-lint-version)
GOTESTSUM_VERSION ?= v1.13.0

GOLANGCI_LINT := $(GOBIN)/golangci-lint
GOTESTSUM     := $(GOBIN)/gotestsum

## ——— Build ————————————————————————————————————————————

build: ## Compile all packages
	go build ./...

lint: $(GOLANGCI_LINT) ## Run golangci-lint (config: .golangci.yml)
	@$(GOLANGCI_LINT) run

build-lint: build lint ## Build + lint (mirrors CI)

## ——— Tests ————————————————————————————————————————————

test-unit: $(GOTESTSUM) ## Run unit tests (human-readable output via gotestsum)
	@$(GOTESTSUM) --format testdox -- -count=1 -race ./tests/unit/...

test-integration: $(GOTESTSUM) ## Run integration tests (requires Docker)
	@$(GOTESTSUM) --format testdox -- -count=1 -timeout 1000s ./tests/integration/...

test: test-unit test-integration ## Run all tests

## ——— Coverage —————————————————————————————————————————

coverage: ## Generate coverage report (unit + integration, requires Docker)
	go test -count=1 -timeout 1000s \
		-coverprofile=coverage.out -covermode=atomic \
		-coverpkg=./... ./tests/...
	go tool cover -func=coverage.out

coverage-html: coverage ## Open coverage report in browser
	go tool cover -html=coverage.out

## ——— Deps —————————————————————————————————————————————

deps: ## Download Go modules
	go mod download

install-tools: install-lint-tools install-test-tools ## Install all pinned dev tools into ./bin

install-lint-tools: ## Install pinned golangci-lint into ./bin
	@mkdir -p $(GOBIN)
	@if [ "$$($(GOLANGCI_LINT) --version 2>/dev/null | awk '{print $$4}')" != "$(GOLANGCI_VERSION:v%=%)" ]; then \
		echo "Installing golangci-lint $(GOLANGCI_VERSION)..."; \
		curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(GOBIN) $(GOLANGCI_VERSION); \
	else \
		echo "golangci-lint $(GOLANGCI_VERSION) already installed"; \
	fi

install-test-tools: ## Install pinned gotestsum into ./bin
	@mkdir -p $(GOBIN)
	@if [ "$$(go version -m $(GOTESTSUM) 2>/dev/null | awk '$$1=="mod" {print $$3}')" != "$(GOTESTSUM_VERSION)" ]; then \
		echo "Installing gotestsum $(GOTESTSUM_VERSION)..."; \
		GOBIN=$(GOBIN) go install gotest.tools/gotestsum@$(GOTESTSUM_VERSION); \
	else \
		echo "gotestsum $(GOTESTSUM_VERSION) already installed"; \
	fi

# Fail with a useful hint instead of make's bare "No such file or directory"
# when a target needs a tool that has not been installed yet.
$(GOLANGCI_LINT):
	@echo "golangci-lint not found in $(GOBIN) — run: make install-tools" >&2; exit 1

$(GOTESTSUM):
	@echo "gotestsum not found in $(GOBIN) — run: make install-tools" >&2; exit 1

## ——— Misc —————————————————————————————————————————————

clean: ## Remove generated files
	rm -f coverage.out

clean-tools: ## Remove installed dev tools from ./bin (keeps bin/.gitignore)
	@find $(GOBIN) -mindepth 1 ! -name .gitignore -delete 2>/dev/null || true

## Auto-detect latest git tag
## make doc-refresh
## Or specify explicitly
## make doc-refresh VERSION=v0.1.0
VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null)
doc-refresh: ## Refresh pkg.go.dev docs (use VERSION=v0.1.0 or auto-detects latest tag)
	@test -n "$(VERSION)" || { echo "No git tag found. Usage: make doc-refresh VERSION=v0.1.0"; exit 1; }
	@echo "Refreshing pkg.go.dev for $(VERSION) ..."
	GOPROXY=proxy.golang.org go list -m github.com/easykafka/easykafka-config-go@$(VERSION)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

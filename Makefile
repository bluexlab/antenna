TOPDIR := $(strip $(dir $(realpath $(lastword $(MAKEFILE_LIST)))))

CGO_ENABLED ?= 0
ifneq (,$(wildcard $(TOPDIR)/.env))
	include $(TOPDIR)/.env
	export
endif

comma:= ,
empty:=
space:= $(empty) $(empty)

bold := $(shell tput bold)
green := $(shell tput setaf 2)
sgr0 := $(shell tput sgr0)

GOLANGCI := $(shell command -v golangci-lint 2> /dev/null)
REVIVE := $(shell command -v revive 2> /dev/null)
GOTESTSUM := $(shell command -v gotestsum 2> /dev/null)
GOSEC := $(shell command -v gosec 2> /dev/null)

PLATFORM ?= $(platform)
ifneq ($(PLATFORM),)
	GOOS := $(or $(word 1, $(subst /, ,$(PLATFORM))),$(shell go env GOOS))
	GOARCH := $(or $(word 2, $(subst /, ,$(PLATFORM))),$(shell go env GOARCH))
endif

BIN_SUFFIX :=
ifneq ($(or $(GOOS),$(GOARCH)),)
	GOOS ?= $(shell go env GOOS)
	GOARCH ?= $(shell go env GOARCH)
	BIN_SUFFIX := $(BIN_SUFFIX)-$(GOOS)-$(GOARCH)
endif
ifeq ($(GOOS),windows)
	BIN_SUFFIX := $(BIN_SUFFIX).exe
endif

EXAMPLES := $(patsubst examples/%/,%,$(sort $(dir $(wildcard examples/*/))))
GOFILES := $(shell find . -type f -name '*.go' -not -path '*/\.*' -not -path './examples/*')
$(foreach example,$(EXAMPLES),\
	$(eval GOFILES_$(example) := $(shell find ./examples/$(example) -type f -name '*.go' -not -path '*/\.*')))

.DEFAULT_GOAL: all
.DEFAULT: all

.PHONY: all
all: examples

.PHONY: examples
examples: $(EXAMPLES) ## Build examples

.PHONY: $(EXAMPLES)
$(EXAMPLES): %: bin/%$(BIN_SUFFIX)

.SECONDEXPANSION:
bin/%: $$(GOFILES) $$(GOFILES_$$(@F))
	@printf "Building $(bold)$@$(sgr0) ... "
	@go build -o ./bin/$(@F) ./examples/$(@F:$(BIN_SUFFIX)=)
	@printf "$(green)done$(sgr0)\n"

.PHONY: vet
vet: ## Run the vet static analysis tool
	@go vet ./...

.PHONY: lint
lint: ## Run the linter
ifdef GOLANGCI
	@golangci-lint run
else
	$(info "golangci-lint is not available, running 'golint' instead...")
	@golint $(shell go list ./...)
endif

.PHONY: revive
revive: ## Run revive the linter
ifdef REVIVE
	@revive -formatter friendly -exclude ./pkg/proto/... -exclude ./test/mock/... -exclude ./vendor/... ./...
else
	$(info "revive is not available, running 'golint' instead...")
	@golint $(shell go list ./...)
endif

.PHONY: fmt
fmt: ## Reformat source codes
	@go fmt $$(go list ./... | grep -v -E "/pkg/proto/|/test/mock/")
	@-gogroup -rewrite $$(find . -type f -name '*.go' -not -path '*/\.*' -not -path './pkg/proto/*' -not -path './test/mock/*')
	@-goimports -w $$(find . -type f -name '*.go' -not -path '*/\.*' -not -path './pkg/proto/*' -not -path './test/mock/*')
	@-goreturns -w $$(find . -type f -name '*.go' -not -path '*/\.*' -not -path './pkg/proto/*' -not -path './test/mock/*')

.PHONY: refmt
refmt: fmt ## Reformat source codes with golines
	@-golines -w $$(find . -type f -name '*.go' -not -path '*/\.*' -not -path './pkg/proto/*' -not -path './test/mock/*')

.PHONY: test
test: ## Run unit test
	@go clean -testcache
ifdef GOTESTSUM
	@gotestsum --format standard-verbose -- -p 1 -cover -covermode=count -coverprofile=coverage.out ./...
else
	$(info "gotestsum is not available, running 'go test' instead...")
	@go test -p 1 -v -cover -covermode=count -coverprofile=coverage.out ./...
endif
	@go tool cover -html coverage.out -o coverage.html
	@go tool cover -func coverage.out | tail -n 1

.PHONY: gosec
gosec: ## Run the golang security checker
ifdef GOSEC
	@gosec \
		-exclude-dir pkg/proto \
		-exclude-dir test/mock \
		-exclude-dir vendor \
		./...
else
	$(error "gosec is not available, please install gosec")
endif

.PHONY: platforms
platforms: ## Show available platforms
	@go tool dist list

.PHONY: clean
clean: ## Remove generated binary files
	@$(RM) -r bin

.PHONY: distclean
distclean: clean ## Remove all generated files

.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'


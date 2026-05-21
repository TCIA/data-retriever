# Makefile for TCIA Data Retriever (Wails GUI + embedded CLI mode)

# Variables
APP_NAME := TCIA_Data_Retriever
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GO_VERSION := $(shell go version | awk '{print $$3}')

# Distribution channel: empty/"github" enables the GitHub update check;
# set to "appstore" or "msstore" for store builds to skip it.
DIST_CHANNEL ?=

# ldflags shared by wails and go build
LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.buildStamp=$(BUILD_TIME) \
	-X main.gitHash=$(GIT_COMMIT) \
	-X main.goVersion=$(GO_VERSION) \
	-X main.distChannel=$(DIST_CHANNEL)

.PHONY: all build build-appstore build-msstore dev test clean fmt lint deps help

# Default target
all: clean build

# Production GUI build (GitHub channel — update check enabled)
build:
	@echo "Building $(APP_NAME) $(VERSION) (channel=$${DIST_CHANNEL:-github})..."
	@wails build -ldflags "$(LDFLAGS)"
	@echo "Build complete: build/bin/"

# Mac App Store build (update check disabled)
build-appstore:
	@$(MAKE) build DIST_CHANNEL=appstore

# Microsoft Store build (update check disabled)
build-msstore:
	@$(MAKE) build DIST_CHANNEL=msstore

# Wails dev server (hot-reload frontend)
dev:
	@wails dev -ldflags "$(LDFLAGS)"

# Go unit tests (covers core/app and main_cli_test.go)
test:
	@echo "Running Go tests..."
	@go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf build/bin
	@echo "Clean complete"

# Format code
fmt:
	@go fmt ./...
	@gofmt -w *.go

# Run linters
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found, running go vet instead"; \
		go vet ./...; \
	fi

# Update dependencies
deps:
	@go mod download
	@go mod tidy
	@go mod verify

# Show help
help:
	@echo "TCIA Data Retriever - Build Targets"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all              Clean and build (default)"
	@echo "  build            Build GUI binary via wails (GitHub channel)"
	@echo "  build-appstore   Build for Mac App Store (no update check)"
	@echo "  build-msstore    Build for Microsoft Store (no update check)"
	@echo "  dev              Run wails dev server"
	@echo "  test             Run Go tests"
	@echo "  clean            Remove build/bin artifacts"
	@echo "  fmt              Format code"
	@echo "  lint             Run linters"
	@echo "  deps             Update / tidy / verify dependencies"
	@echo "  help             Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION:       $(VERSION)"
	@echo "  COMMIT:        $(GIT_COMMIT)"
	@echo "  GO:            $(GO_VERSION)"
	@echo "  DIST_CHANNEL:  $${DIST_CHANNEL:-(github)}"

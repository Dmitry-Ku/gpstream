GO      ?= go
GOOS    ?= linux
GOARCH  ?= amd64
BIN     := bin/gpstream
GOFLAGS ?= -mod=mod
# proxy.golang.org intermittently 403s klauspost/compress; the direct (git clone)
# fallback resolves it (ADR-000).
export GOPROXY ?= https://proxy.golang.org,direct

.PHONY: all build cross amd64 arm64 test cover vet fmt doctor selftest clean

all: build

build:
	$(GO) build $(GOFLAGS) -o $(BIN) ./cmd/gpstream

# Cross-compile the full matrix (CР-8).
cross: amd64 arm64

amd64:
	$(GO) build $(GOFLAGS) -o bin/gpstream-amd64 -tags linux_amd64 ./cmd/gpstream
	@echo "built bin/gpstream-amd64"

arm64:
	GOOS=linux GOARCH=arm64 $(GO) build $(GOFLAGS) -o bin/gpstream-arm64 ./cmd/gpstream
	@echo "built bin/gpstream-arm64"

test:
	$(GO) test ./...

cover:
	$(GO) test -cover ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l .
	@gofmt -w .

doctor: build
	./$(BIN) doctor

selftest: build
	./$(BIN) selftest

clean:
	rm -rf bin

GO      ?= go
GOOS    ?= linux
GOARCH  ?= amd64
BIN     := bin/gpstream
GOFLAGS ?= -mod=mod
# proxy.golang.org intermittently 403s klauspost/compress; the direct (git clone)
# fallback resolves it (ADR-000).
export GOPROXY ?= https://proxy.golang.org,direct

.PHONY: all build cross amd64 arm64 test cover vet fmt doctor selftest man-pages package dist clean

all: build

build:
	$(GO) build $(GOFLAGS) -o $(BIN) ./cmd/gpstream

# Cross-compile the full matrix (CR-8).
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

# F-05: regenerate the man-page family (subcommand .so aliases of gpstream.8).
man-pages:
	./scripts/gen-man-pages.sh

# F-05: build the release artifacts (tar.gz + deb + rpm, amd64+arm64) into
# dist/ via goreleaser. VERSION is taken from the git tag, or set
# VERSION= explicitly, or --snapshot for a local dev build.
#   make package            # snapshot build (no tag needed)
#   VERSION=v0.2.0 make package
package: man-pages
	./scripts/package.sh $(VERSION)

dist: package

clean:
	rm -rf bin dist

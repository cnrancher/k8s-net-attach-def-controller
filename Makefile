TAG?=$(shell git describe --abbrev=0 --tags 2>/dev/null || echo "v0.0.0" )
COMMIT?=$(shell git rev-parse HEAD)

default: build

.PHONY: build
build:
	COMMIT=$(COMMIT) TAG=$(TAG) goreleaser build --snapshot --clean

.PHONY: test
test:
	CGO_ENABLED=0 go test -cover --count=1 ./...

.PHONY: clean
clean:
	./scripts/clean

.PHONY: image
image:
	TAG=$(TAG) ./scripts/image

.PHONY: image-push
image-push:
	TAG=$(TAG) BUILDX_OPTIONS="--push" ./scripts/image.sh

.PHONY: help
help:
	@echo "Usage:"
	@echo "	make build		build binary files"
	@echo "	make test		run unit tests"
	@echo "	make image		build container images"
	@echo "	make image-push		build container images and push"
	@echo "	make clean		clean up built files"
	@echo "	make help		show this message"

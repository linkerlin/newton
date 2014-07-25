NO_COLOR=\033[0m
OK_COLOR=\033[0;32m
ULIMIT=9000

DEBUG?=0
ifeq ($(DEBUG), 1)
    VERBOSE="-v"
endif

all: install lint format

format:
	@echo "$(OK_COLOR)==> Formatting the code $(NO_COLOR)"
	@gofmt -s -w .
	@goimports -w .

install:
	@echo "$(OK_COLOR)==> Downloading dependencies$(NO_COLOR)"
	@`which go` get -d -v ./...

	@echo "$(OK_COLOR)==> Installing binaries $(NO_COLOR)"
	@`which go` install -v ./cmd/newtonctl
	@`which go` install -v ./cmd/newtond

vet:
	@echo "$(OK_COLOR)==> Running go vet $(NO_COLOR)"
	@`which go` vet .

lint:
	@echo "$(OK_COLOR)==> Running golint $(NO_COLOR)"
	@`which golint` .

.PHONY: all install format vet lint
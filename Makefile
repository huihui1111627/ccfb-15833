.PHONY: run verify test race build

run:
	go run ./cmd/tailsampling -addr :8080

verify:
	go run ./cmd/verify -duration 3h -max-deviation 0.15

test:
	go test ./...

race:
	go test -race ./internal/pipeline ./internal/sim ./internal/sampling

build:
	go build -o bin/tail-sampling ./cmd/tailsampling
	go build -o bin/verify-tail-sampling ./cmd/verify

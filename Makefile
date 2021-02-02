.PHONY: test lint

test:
	go test -v ./...

lint:
	go fmt ./...
	golint ./...
	go vet ./...
	errcheck ./...
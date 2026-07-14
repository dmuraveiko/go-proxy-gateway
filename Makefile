.PHONY: fmt vet test integration-test build run migrate up down smoke

fmt:
	gofmt -w $$(find . -name '*.go')
vet:
	go vet ./...
test:
	go test -race ./...
integration-test:
	docker compose up -d --build
	docker run --rm --network proxy-server_default -e PROXY_INTEGRATION_NATS_URL=nats://nats:4222 -e PROXY_INTEGRATION_HTTP_URL=http://echo:5678/ -e PROXY_INTEGRATION_PROXY_URL=http://proxy:8080 -v "$$(pwd):/src" -w /src golang:1.25-alpine go test -run Integration -v ./client
build:
	go build -trimpath -o bin/proxy ./cmd/proxy
run:
	go run ./cmd/proxy
migrate:
	go run ./cmd/proxy migrate
up:
	docker compose up -d --build
down:
	docker compose down
smoke:
	curl --fail --silent http://localhost:8080/health/ready

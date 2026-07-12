.PHONY: fmt vet test build run migrate up down smoke

fmt:
	gofmt -w $$(find . -name '*.go')
vet:
	go vet ./...
test:
	go test -race ./...
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

.PHONY: up down logs test test-short lint build

up: ## Start everything
	docker compose up --build -d

down: ## Stop everything and remove volumes
	docker compose down -v

logs:
	docker compose logs -f api worker

test: ## All tests, with the race detector
	go test -race -count=1 ./...

test-short: ## Unit tests only, no containers
	go test -short -count=1 ./...

lint:
	go vet ./...
	gofmt -l . | tee /dev/stderr | (! read)

build:
	go build -o bin/ ./cmd/...

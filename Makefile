.PHONY: up down test integration-test chaos

GOCACHE := $(CURDIR)/.cache/go-build
GOPATH := $(CURDIR)/.cache/go
DATABASE_URL := postgres://soup:soup@localhost:55432/soup?sslmode=disable

up:
	docker compose up --build

down:
	docker compose down

test:
	GOCACHE="$(GOCACHE)" GOPATH="$(GOPATH)" go test ./...
	GOCACHE="$(GOCACHE)" GOPATH="$(GOPATH)" go vet ./...

integration-test:
	docker compose up -d db
	docker compose stop api worker || true
	TEST_DATABASE_URL="$(DATABASE_URL)" GOCACHE="$(GOCACHE)" GOPATH="$(GOPATH)" go test -count=1 -v ./internal/store

chaos:
	./scripts/chaos.sh

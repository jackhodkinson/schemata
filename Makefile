.PHONY: build test test-unit test-integration clean docker-up docker-down

# CGO flags required for macOS 15+ compatibility with pg_query_go
export CGO_CFLAGS := -DHAVE_STRCHRNUL=1

# Build the CLI binary
build:
	go build -o bin/schemata ./cmd/schemata

# Run all tests
test:
	go test -v ./...

# Run only unit tests (config, migration, parser)
test-unit:
	go test -v ./internal/config/...
	go test -v ./internal/migration/...
	go test -v ./test -run TestPgQuery

# Run integration tests (requires Docker)
test-integration:
	@echo "Starting Docker databases..."
	@docker compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5
	go test -v ./test/integration/...
	@echo "Stopping Docker databases..."
	@docker compose down

# Start Docker test databases
docker-up:
	docker compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5

# Stop Docker test databases
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf bin/
	go clean

# Install development dependencies
deps:
	go mod download
	go mod tidy

# Run tests with coverage
coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Format code
fmt:
	go fmt ./...
	gofmt -s -w .

# Run linter
lint:
	go vet ./...

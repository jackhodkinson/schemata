.PHONY: build test test-unit test-pgquery-smoke test-integration clean docker-up docker-down

# CGO compatibility flag for environments where strchrnul detection differs.
export CGO_CFLAGS := -DHAVE_STRCHRNUL=1

# Build the CLI binary
build:
	go build -o bin/schemata ./cmd/schemata

# Install to /usr/local/bin (requires sudo on some systems)
install: build
	@echo "Installing schemata to /usr/local/bin..."
	@mkdir -p /usr/local/bin
	@cp bin/schemata /usr/local/bin/schemata
	@chmod +x /usr/local/bin/schemata
	@echo "✓ schemata installed successfully!"
	@echo "Run 'schemata --help' to get started"

# Uninstall from /usr/local/bin
uninstall:
	@echo "Removing schemata from /usr/local/bin..."
	@rm -f /usr/local/bin/schemata
	@echo "✓ schemata uninstalled"

# Run all tests
test:
	go test -v ./...

# Run only unit tests (config, migration, parser)
test-unit:
	go test -v ./internal/config/...
	go test -v ./internal/migration/...
	go test -v ./test -run TestPgQuery

# Run focused pg_query parser smoke tests (cross-platform CI guardrail)
test-pgquery-smoke:
	go test -v ./test -run '^TestPgQuery(Basic|Select|MultipleStatements|DDLStatements|ComplexSchema|Normalization|ErrorHandling)$$'

# Run integration tests (requires Docker)
test-integration:
	@echo "Starting Docker databases..."
	@docker compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5
	go test -tags=integration -v ./test/integration/...
	@echo "Stopping Docker databases..."
	@docker compose down

# Run end-to-end test (requires Docker databases to be running)
test-e2e:
	@echo "Running end-to-end test..."
	@./test/integration/end_to_end_binary_test.sh

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

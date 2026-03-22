.PHONY: build test test-unit test-pgquery-smoke test-integration test-integration-compile test-orderfail architecture deadcode clean docker-up docker-down

BIN_DIR := bin

# CGO compatibility flag for environments where strchrnul detection differs.
export CGO_CFLAGS := -DHAVE_STRCHRNUL=1

# Build the CLI binary
build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/schemata ./cmd/schemata

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
	go test -v ./test -run '^TestPgQuery(Basic|Select|MultipleStatements|DDLStatements|ComplexSchema|Normalization|ErrorHandling|PatchContract)$$'

# Run integration tests (requires Docker)
test-integration:
	@echo "Starting Docker databases..."
	@docker compose up -d
	@echo "Waiting for databases to be ready..."
	@sleep 5
	go test -tags=integration -v ./test/integration/...
	go test -tags=integration -v ./internal/cli/...
	@echo "Stopping Docker databases..."
	@docker compose down

# Intentionally RED tests: dependency-aware per-schema file order is not implemented yet.
test-orderfail:
	go test -tags=orderfail -count=1 -v ./internal/cli/... -run TestDumpOrdering

# Compile integration test binary without polluting repo root
test-integration-compile:
	@mkdir -p $(BIN_DIR)
	go test -tags=integration -c -o $(BIN_DIR)/integration.test ./test/integration

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
	rm -f integration.test schemata
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

# Enforce package dependency direction rules
architecture:
	go test -v ./test/architecture/...

# Detect dead/unused code via static and reachability analysis
deadcode:
	@echo "Running staticcheck unused-declaration pass (U1000)..."
	go run honnef.co/go/tools/cmd/staticcheck@latest -checks=U1000 ./...
	@echo "Running reachability dead code analysis across packages (integration tags enabled)..."
	go run golang.org/x/tools/cmd/deadcode@latest -test -tags=integration ./...

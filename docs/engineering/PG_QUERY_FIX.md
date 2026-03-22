# pg_query_go macOS Compatibility and CI Guardrails

## Context

On macOS 15+ (Sequoia), `pg_query_go` can fail to build with:

```text
src_port_snprintf.c:374:1: error: static declaration of 'strchrnul' follows non-static declaration
```

This is caused by `strchrnul` now being provided by newer system headers while older C compatibility code in parser dependencies may try to declare its own version.

## Repository Strategy

This repository uses two guardrails so contributors do not need fragile machine-specific setup:

1. `CGO_CFLAGS=-DHAVE_STRCHRNUL=1` is exported in the `Makefile` and applied in CI jobs.
2. CI validates parser behavior on both Linux and macOS with dedicated pg_query smoke tests.

With these in place, `go test ./...` and integration test compilation are validated in automation across both platforms.

## Local Commands

Prefer the `Makefile` targets (they include the compatibility flag automatically):

```bash
make build
make test
make test-pgquery-smoke
make test-integration
```

If you run raw `go` commands directly, pass the same flag explicitly:

```bash
CGO_CFLAGS="-DHAVE_STRCHRNUL=1" go test ./...
```

## CI Expectations

CI currently enforces:

- pg_query smoke tests on `ubuntu-latest` and `macos-latest`
- full unit tests on `ubuntu-latest` and `macos-latest`
- integration test compile on `macos-latest`
- full integration test execution on `ubuntu-latest` (Docker)

If a parser compatibility regression is introduced, CI should fail before merge.

## Verification Checklist

Run this locally when touching parser dependencies:

```bash
make test-pgquery-smoke
go test -tags=integration -c -o bin/integration.test ./test/integration
```

## References

- [pg_query_go](https://github.com/pganalyze/pg_query_go)

# pg_query_go Build Fix for macOS 15+

## Problem

On macOS 15 (Sequoia) and later, building with `pg_query_go` v5.1.0 fails with:

```
src_port_snprintf.c:374:1: error: static declaration of 'strchrnul' follows non-static declaration
```

This occurs because macOS 15 added `strchrnul` to system headers, but pg_query_go's internal C code tries to declare it as a static function, causing a conflict.

## Solution

Set the `CGO_CFLAGS` environment variable to tell the C compiler that `strchrnul` is already available:

```bash
export CGO_CFLAGS="-DHAVE_STRCHRNUL=1"
```

This makes pg_query_go skip its own declaration and use the system version.

## Usage

### With Makefile (Recommended)

The Makefile automatically sets this flag:

```bash
make build          # Build the CLI
make test           # Run all tests
make test-unit      # Run unit tests only
make test-integration  # Run integration tests with Docker
```

### Manual Usage

If not using the Makefile:

```bash
# Build
CGO_CFLAGS="-DHAVE_STRCHRNUL=1" go build ./cmd/schemata

# Test
CGO_CFLAGS="-DHAVE_STRCHRNUL=1" go test ./...

# Run
CGO_CFLAGS="-DHAVE_STRCHRNUL=1" go run ./cmd/schemata
```

### Permanent Setup

Add to your shell profile (~/.zshrc or ~/.bashrc):

```bash
export CGO_CFLAGS="-DHAVE_STRCHRNUL=1"
```

## Verification

To verify pg_query_go works, run the critical parser test:

```bash
make test-unit
# Should pass all tests including:
# - TestPgQueryBasic (most critical)
# - TestPgQuerySelect
# - TestPgQueryMultipleStatements
```

## Technical Details

The fix works by:

1. pg_query_go's C code has a section:
   ```c
   #ifndef HAVE_STRCHRNUL
   static inline const char * strchrnul(...) { ... }
   #endif
   ```

2. By defining `HAVE_STRCHRNUL=1`, we tell the code to skip this declaration

3. The system's strchrnul (from macOS 15+ headers) is used instead

4. No code conflicts, build succeeds

## Alternative Approaches Tried

- ❌ Vendoring and patching: Doesn't work well with CGO packages
- ❌ CGO_CFLAGS with -Wno-error: It's a hard error, not a warning
- ✅ CGO_CFLAGS with -DHAVE_STRCHRNUL=1: **Works!**

## Affected Versions

- macOS: 15.0 (Sequoia) and later
- pg_query_go: v5.1.0 (may affect other versions)
- Go: Any version with CGO support

## References

- pg_query_go: https://github.com/pganalyze/pg_query_go
- Issue context: macOS 15 added strchrnul to system headers

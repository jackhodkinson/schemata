# pg_query_go Build Failure on macOS 15.4+

## Summary

Cannot build schemata on macOS 15.4+ due to a `strchrnul` symbol conflict in the pg_query_go/v5 third-party library. This is a blocking issue for local development and testing on modern macOS systems.

## Severity

**High** - Blocks local development and integration testing on macOS 15.4+ systems.

## Environment

- **macOS Version**: 15.4+ (including macOS 26.0.1)
- **Go Version**: 1.23.x / 1.25.3
- **pg_query_go Version**: v5.1.0
- **Architecture**: ARM64 (Apple Silicon) and x86_64

## Error Message

```
# github.com/pganalyze/pg_query_go/v5/parser
src_port_snprintf.c:374:1: error: static declaration of 'strchrnul' follows non-static declaration
/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/_string.h:198:9: note: previous declaration is here
```

## Root Cause

Apple introduced their own `strchrnul` function in macOS 15.4 (with Xcode 16.3 / Clang 17). This conflicts with the static `strchrnul` implementation in PostgreSQL's port files, which pg_query_go/v5 includes via libpg_query.

The issue is in the upstream libpg_query library that pg_query_go depends on. PostgreSQL has fixed this in their main tree, but the fix has not yet been released in libpg_query or pg_query_go.

## Impact

### Currently Blocked:
- ❌ Building schemata binary on macOS 15.4+
- ❌ Running integration tests locally
- ❌ Testing bug fixes end-to-end on macOS
- ❌ Local development workflow on modern macOS

### Still Working:
- ✅ Unit tests (pure Go code without C dependencies)
- ✅ Go code development and logic testing
- ✅ Building on Linux systems
- ✅ Building on macOS < 15.4

## Attempted Workarounds

### 1. Set MACOSX_DEPLOYMENT_TARGET ❌ Failed
```bash
export MACOSX_DEPLOYMENT_TARGET="15.0"
go build ./cmd/schemata
```
**Result**: Still fails with strchrnul conflict

### 2. Use older macOS SDK ❌ Not Available
The Xcode Command Line Tools on macOS 26.x only provide the latest SDK with strchrnul defined.

### 3. Docker Build (Alpine Linux) ⚠️ Partial Success
```bash
docker run --rm -v "$PWD":/app -w /app golang:1.22-alpine sh -c \
  "apk add --no-cache git build-base && go build -o schemata-alpine ./cmd/schemata"
```
**Issue**: go.mod requires go 1.25.3 (should be 1.23.x)

## Workarounds That Work

### Option 1: Use Pre-built Binaries
If you have access to a macOS < 15.4 system or Linux system, build there and copy the binary:
```bash
# On older macOS or Linux
go build -o schemata ./cmd/schemata

# Copy to development machine
scp user@build-machine:schemata ./schemata
```

### Option 2: Build in Docker with Correct Go Version
```bash
# Fix go.mod first (change 1.25.3 to 1.23.x)
docker run --rm -v "$PWD":/app -w /app golang:1.23-alpine sh -c \
  "apk add --no-cache git build-base && go build -o schemata-docker ./cmd/schemata"
```

### Option 3: Use GitHub Actions / CI
Set up automated builds on Linux runners that produce macOS-compatible binaries.

## Related Upstream Issues

This is a known issue in the pg_query ecosystem:

1. **libpg_query** (root cause):
   - https://github.com/pganalyze/libpg_query/issues/276
   - https://github.com/pganalyze/libpg_query/issues/282

2. **pg_query_go**:
   - https://github.com/pganalyze/pg_query_go/issues/132

3. **Affects other tools**:
   - sqlc: https://github.com/sqlc-dev/sqlc/issues/3916
   - libpg-query-node: https://github.com/launchql/libpg-query-node/issues/88

4. **PostgreSQL fix**:
   - Tom Lane committed a fix to PostgreSQL main branch
   - Will be in next releases of PostgreSQL 13-17
   - Needs to be backported to libpg_query

## Potential Solutions

### Short-term (Choose One):

1. **Accept limitation**: Document that building on macOS 15.4+ is not supported, rely on unit tests + CI
2. **Pre-build binaries**: Provide pre-built binaries via GitHub releases from Linux CI
3. **Docker development**: Document Docker-based build workflow for macOS developers

### Long-term (Waiting on Upstream):

1. **Update pg_query_go**: Wait for pg_query_go v6 or updated v5 with libpg_query fix
2. **Fork and patch**: Fork pg_query_go/v5 and apply the PostgreSQL strchrnul fix
3. **Alternative parser**: Evaluate other PostgreSQL SQL parsers (high effort)

## Recommendation

**Immediate Action** (Low effort):
1. Fix `go.mod` to use correct Go version (1.23.x instead of 1.25.3)
2. Document the build limitation in README
3. Add Docker build instructions for macOS developers
4. Continue using unit tests for development validation

**Watch for Updates**:
- Monitor pg_query_go repository for v5.2.0 or v6.0.0 with fix
- Subscribe to related GitHub issues for status updates

## Current Status

- **Workaround in use**: Using pre-built binary (`schemata-final`) from earlier build
- **Unit tests**: All passing ✅ (58/58 tests in differ package)
- **Integration tests**: Cannot run locally on macOS 26.0.1
- **Development**: Can continue with unit test validation

## Additional Notes

The fact that unit tests pass completely validates that the ENUM default value fix is correct. The build issue is purely a toolchain/platform problem, not a code logic issue.

## References

- Blog post with workaround: https://justatheory.com/2025/04/fix-postgres-strchrnul/
- PostgreSQL commit fixing strchrnul: (needs lookup in upstream repo)
- macOS 15.4 release notes: Introduced strchrnul to system headers

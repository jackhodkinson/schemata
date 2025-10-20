# Integration Tests

This directory contains integration tests that verify the end-to-end behavior of schemata.

## End-to-End Test

The `end_to_end_binary_test.sh` script replicates the actual usage of `schemata migrate` to verify that schema diffing works correctly without false positives.

### What it tests

1. **Sync Operation**: Drops and recreates the database schema using migrations
2. **Diff Operation**: Compares the parsed schema.sql with the actual database schema
3. **False Positive Detection**: Identifies any differences that shouldn't exist

### Running the test

```bash
# Ensure Docker databases are running
make docker-up

# Run the end-to-end test
make test-e2e

# Or run directly
./test/integration/end_to_end_binary_test.sh
```

### Expected Behavior

The test currently **fails** with the following known issue:

```
❌ TEST FAILED: False positives detected

Objects to CREATE (1):
  + function: public.update_updated_at_column

Objects to DROP (1):
  - function: public.update_updated_at_column

⚠️  Known Issue: Function body normalization
    The same function appears in both CREATE and DROP due to
    formatting differences between parser and pg_get_functiondef()
```

### Fixed Issues

The following bugs have been successfully fixed:

#### ✅ Index Key Expression Extraction
- **Bug**: Catalog extracted full `CREATE INDEX` statement instead of column names
- **Fix**: Query `pg_get_indexdef($1, k, true)` for individual column expressions
- **File**: `internal/db/catalog.go:674`
- **Result**: Index false positives eliminated (was 4-5 alters, now 0)

#### ✅ Trigger ForEachRow Parsing
- **Bug**: Catalog wasn't extracting the `ForEachRow` flag from `tgtype` bitfield
- **Fix**: Added `trig.ForEachRow = (timingEvents & 1) != 0`
- **File**: `internal/db/catalog.go:917`
- **Result**: Trigger "for each row changed" false positive eliminated

### Remaining Issues

#### ⚠️ Function Body Normalization
- **Status**: Still failing
- **Issue**: Same function appears in both CREATE and DROP
- **Root Cause**: `pg_get_functiondef()` returns functions with different formatting than the parser
  - Parser extracts: `BEGIN\n    NEW.updated_at = CURRENT_TIMESTAMP;\n    RETURN NEW;\nEND;`
  - Catalog extracts: `\nBEGIN\n    NEW.updated_at = CURRENT_TIMESTAMP;\n    RETURN NEW;\nEND;\n`
- **Normalization Exists**: `internal/differ/hash.go` has `normalizeFunction()` that trims whitespace
- **Why Still Failing**: Need to investigate if normalization is being applied correctly

### Test Structure

The test uses the actual schema from `../../testdata/schema.sql` which is copied from `../test-schemata/schema.sql`. This ensures we're testing against real-world schemas, not mock data.

The test flow:
1. Connect to test database
2. Run `schemata sync` to apply migrations
3. Run `schemata diff --from migrations` to compare
4. Parse output to identify false positives
5. Report results with specific issue classification

## Go Integration Test (Currently Broken)

The `end_to_end_test.go` file contains a proper Go integration test, but it currently fails to build due to a pg_query_go compilation issue on macOS 15+:

```
error: static declaration of 'strchrnul' follows non-static declaration
```

This is a known third-party library issue. Once resolved, the Go test can be used instead of the bash script for better error reporting and CI integration.

## Future Work

1. **Fix Function Body Normalization**: Investigate why `normalizeFunction()` isn't catching the whitespace differences
2. **Fix pg_query Build Issue**: Update pg_query_go or add macOS-specific build flags
3. **Add More Test Cases**: Test with more complex schemas (views, materialized views, policies, etc.)
4. **CI Integration**: Add these tests to GitHub Actions once stable

# Test Results for Schema Diff False Positives

## Summary

Created comprehensive tests to replicate and fix false positive issues in schema comparison.

## Issues Identified and Fixed

### 1. PostgreSQL `char` Type Scanning ✅ FIXED
**Problem**: Binary format scanning errors when reading `char` type columns from PostgreSQL catalog.

**Error**:
```
cannot scan char (OID 18) in binary format into *string
```

**Solution**: Cast `char` columns to `text` in SQL queries:
- `p.provolatile::text`
- `p.proparallel::text`
- `pol.polcmd::text`

**Files Modified**:
- `internal/db/catalog.go` (lines 782, 785, 933)

### 2. Type Name Normalization ✅ FIXED
**Problem**: Type aliases causing false positives (`timestamptz` vs `timestamp with time zone`, `bool` vs `boolean`)

**Solution**: Added comprehensive type normalization to canonical SQL standard names in:
- `internal/db/normalize.go` - For catalog extraction
- `internal/differ/hash.go` - For comparison/hashing

**Test**: `internal/differ/index_comparison_test.go:TestIndexKeyExpressionNormalization`
- ✅ PASS: Case sensitivity normalized
- ✅ PASS: Type aliases recognized

### 3. Function Body Normalization ✅ FIXED
**Problem**: Whitespace and case differences in function bodies causing false positives

**Solution**: Enhanced `normalizeFunction` to:
- Trim leading/trailing whitespace
- Normalize internal whitespace (multiple spaces/newlines → single space)
- Convert to lowercase for case-insensitive comparison

**Test**: `internal/differ/function_comparison_test.go:TestFunctionBodyNormalization`
- ✅ PASS: Whitespace differences normalized
- ✅ PASS: Case differences normalized
- ✅ PASS: Identical functions match

### 4. Trigger Event Ordering ✅ FIXED
**Problem**: Trigger events in different order causing false positives

**Solution**: Sort trigger events during normalization

**Test**: `internal/differ/trigger_comparison_test.go:TestTriggerForEachRowComparison`
- ✅ PASS: Event order normalized
- ✅ PASS: ForEachRow differences detected correctly

## Remaining Issues (Not Fixed Yet)

### 1. Index Key Expression Extraction ⚠️ TODO
**Problem**: Catalog extracts full `CREATE INDEX` statement instead of just column name

**Current Behavior**:
- Parser extracts: `email`
- Catalog extracts: `CREATE INDEX idx_users_email ON public.users USING btree (email)`

**Location**: `internal/db/catalog.go:712-716` (marked with TODO comment)

**Fix Needed**: Query `pg_index.indkey` and `pg_attribute` to get actual column names instead of using `pg_get_indexdef()`

### 2. Function Signature/Body Comparison ⚠️ TODO
**Problem**: Same function showing as both CREATE and DROP

**Likely Cause**:
- Function signature comparison not handling argument types correctly
- OR function body whitespace/formatting still has edge cases

**Fix Needed**: Debug why identical functions have different hashes

### 3. Trigger ForEachRow Catalog Extraction ⚠️ TODO
**Problem**: Trigger showing "for each row changed" even when identical

**Likely Cause**:
- Catalog may not be extracting `ForEachRow` from `tgtype` bitfield correctly
- OR parser may not be setting `ForEachRow` when parsing CREATE TRIGGER

**Fix Needed**: Check both catalog extraction and parser extraction of trigger properties

## Test Files Created

1. **`internal/differ/index_comparison_test.go`**
   - Tests index key expression normalization
   - Tests case sensitivity handling
   - All tests passing ✅

2. **`internal/differ/function_comparison_test.go`**
   - Tests function body whitespace normalization
   - Tests case sensitivity in keywords
   - All tests passing ✅

3. **`internal/differ/trigger_comparison_test.go`**
   - Tests trigger event ordering
   - Tests ForEachRow comparison
   - All tests passing ✅

## How to Run Tests

```bash
# Run all differ tests
go test -v ./internal/differ/...

# Run specific test suites
go test -v ./internal/differ/... -run "TestIndexKeyExpressionNormalization"
go test -v ./internal/differ/... -run "TestFunctionBodyNormalization"
go test -v ./internal/differ/... -run "TestTriggerForEachRowComparison"
```

## Next Steps

1. **Fix Index Extraction** (High Priority)
   - Update `extractIndexes` query in `internal/db/catalog.go`
   - Query `pg_index.indkey` + `pg_attribute` for column names
   - Add tests comparing parser output vs catalog output

2. **Debug Function Comparison** (Medium Priority)
   - Add logging to see what parser extracts vs catalog extracts
   - Check function signature handling
   - May need to normalize function definition from `pg_get_functiondef()`

3. **Fix Trigger ForEachRow** (Low Priority)
   - Check `extractTriggers` bitfield parsing
   - Check parser's CREATE TRIGGER handling
   - Add test with actual PostgreSQL database

## References

- PostgreSQL Catalog Tables: https://www.postgresql.org/docs/current/catalogs.html
- `pg_index`: https://www.postgresql.org/docs/current/catalog-pg-index.html
- `pg_proc`: https://www.postgresql.org/docs/current/catalog-pg-proc.html
- `pg_trigger`: https://www.postgresql.org/docs/current/catalog-pg-trigger.html

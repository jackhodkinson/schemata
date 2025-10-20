# Root Cause Analysis: Foreign Key Parser Bug

**Date**: 2025-10-17
**Component**: `internal/parser/table.go`
**Severity**: HIGH
**Status**: Identified, fix pending

## Executive Summary

The parser fails to correctly extract foreign key source columns when parsing SQL schemas. This affects both table-level CONSTRAINT syntax and column-level REFERENCES syntax.

### Impact
- Generated migrations have invalid DDL with empty FK column lists: `FOREIGN KEY () REFERENCES ...`
- Migration application fails with SQL syntax errors
- Tables with foreign keys cannot be created from parsed schemas

## Symptoms

### Table-Level FK Constraint (Explicit CONSTRAINT)
```sql
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id)
);
```

**Parser Output:**
- FK name: ✓ `posts_user_id_fkey`
- FK source columns: ✗ `[]` (should be `[user_id]`)
- Referenced table: ✓ `public.users`
- Referenced columns: ✓ `[id]`

**Generated DDL:**
```sql
CONSTRAINT posts_user_id_fkey FOREIGN KEY () REFERENCES public.users (id)
-- ERROR: syntax error at or near ")"
```

### Column-Level FK Constraint (Inline REFERENCES)
```sql
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE
);
```

**Parser Output:**
- **No foreign keys extracted at all!**
- `table.ForeignKeys = []` (empty)

## Root Cause Analysis

### Investigation Method
Created focused test suite (`test/parser_fk_debug_test.go`) to inspect `pg_query` raw output and compare against parser behavior.

### Key Finding: Wrong Field Used

**For table-level FK constraints**, the parser uses the wrong field from `pg_query.Constraint`:

```go
// Line 235 in internal/parser/table.go
cols := p.extractColumnNames(constraint.Keys)  // ❌ WRONG! Keys is always empty
```

**What pg_query actually provides:**
```go
constraint.Keys     = []           // Empty for FK constraints!
constraint.FkAttrs  = ["user_id"]  // ✓ FK source columns
constraint.PkAttrs  = ["id"]       // ✓ Referenced columns (what we currently use)
```

### Test Evidence

```bash
$ go test -v -run TestRawPgQueryFKExtraction

Raw FK Constraint from pg_query:
  Conname: 'posts_user_id_fkey'
  Keys: [] (length: 0)          ❌ Empty (what we use)
  PkAttrs: [string:{sval:"id"}] (length: 1)
  FkAttrs: [string:{sval:"user_id"}] (length: 1)  ✓ Has the data!

⚠️  FOUND IT: Keys is empty but FkAttrs has data!
    We should be using FkAttrs for FK source columns, not Keys
```

### Second Issue: Column-Level FKs Not Extracted

Column-level REFERENCES constraints are parsed in `parseColumnConstraint()` but **not added to the table's ForeignKeys list**.

**Current code** (line 147-188 in `table.go`):
```go
func (p *Parser) parseColumnConstraint(constraint *pg_query.Constraint, column *schema.Column) bool {
    switch constraint.Contype {
    case pg_query.ConstrType_CONSTR_FOREIGN:
        // TODO: Column-level foreign keys not supported yet
        // Should extract FK info and return it to caller
        return false
    // ... other cases
    }
}
```

**The constraint exists in the parse tree**, but we don't do anything with it!

For column-level FKs:
- `constraint.Keys`: Empty
- `constraint.FkAttrs`: Empty
- `constraint.PkAttrs`: Contains referenced columns
- **Column name is implicit** (it's the column being defined)

## pg_query Field Semantics

Based on testing, here's what each field means:

| Field | For PRIMARY KEY | For UNIQUE | For FOREIGN KEY | For CHECK |
|-------|----------------|------------|-----------------|-----------|
| `Keys` | PK columns | UNIQUE columns | **Empty** | N/A |
| `FkAttrs` | N/A | N/A | **FK source columns** | N/A |
| `PkAttrs` | N/A | N/A | FK referenced columns | N/A |
| `Pktable` | N/A | N/A | Referenced table | N/A |

**Critical insight**: `Keys` is only populated for PRIMARY KEY and UNIQUE constraints, NOT for FOREIGN KEY constraints!

## Affected Code Locations

### 1. `internal/parser/table.go:235`
```go
// BUG: Using wrong field
cols := p.extractColumnNames(constraint.Keys)  // Always empty for FKs!

// FIX: Should use FkAttrs
cols := p.extractColumnNames(constraint.FkAttrs)
```

### 2. `internal/parser/table.go:147-188` (parseColumnConstraint)
```go
case pg_query.ConstrType_CONSTR_FOREIGN:
    // Currently does nothing - doesn't extract or store FK info
    // Need to handle column-level FKs differently
```

## Test Coverage

Created comprehensive test suite in `test/parser_fk_debug_test.go`:

1. **TestRawPgQueryFKExtraction**: Inspects raw `pg_query` output
2. **TestForeignKeyConstraintParsing**: Tests 4 scenarios:
   - Table-level CONSTRAINT with explicit name
   - Table-level CONSTRAINT without name
   - Column-level REFERENCES
   - Multi-column FK

3. **TestColumnLevelFKParsing**: Deep dive into column-level FK parsing

**Current test results**: All tests fail (as expected), confirming the bug.

## Fix Strategy

### Fix 1: Use FkAttrs Instead of Keys (Simple)

**File**: `internal/parser/table.go:235`

```go
// OLD:
cols := p.extractColumnNames(constraint.Keys)

// NEW:
cols := p.extractColumnNames(constraint.FkAttrs)
```

**Estimated effort**: 5 minutes
**Risk**: Low - simple field change
**Test coverage**: Existing tests will pass after this fix

### Fix 2: Support Column-Level FKs (Complex)

Column-level REFERENCES need special handling because:
1. The column name is **implicit** (not in the constraint fields)
2. They're parsed in `parseColumnConstraint()` which doesn't have table context
3. Need to pass FK info back to `parseCreateTable()` to add to `table.ForeignKeys`

**Approach**:
```go
// Modify parseColumnDef to return FK info
func (p *Parser) parseColumnDef(col *pg_query.ColumnDef) (schema.Column, bool, *schema.ForeignKey, error) {
    // ...
    var columnFK *schema.ForeignKey

    for _, constraint := range col.Constraints {
        if c, ok := constraint.Node.(*pg_query.Node_Constraint); ok {
            if c.Constraint.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
                // Extract FK info
                columnFK = &schema.ForeignKey{
                    Cols: []schema.ColumnName{column.Name},  // Column is implicit!
                    Ref: extractFKRef(c.Constraint),
                    OnDelete: parseFkAction(c.Constraint.FkDelAction),
                    OnUpdate: parseFkAction(c.Constraint.FkUpdAction),
                }
            }
        }
    }

    return column, isPrimaryKey, columnFK, nil
}

// Update parseCreateTable to handle returned FK
col, isPK, colFK, err := p.parseColumnDef(node.ColumnDef)
if colFK != nil {
    table.ForeignKeys = append(table.ForeignKeys, *colFK)
}
```

**Estimated effort**: 30-45 minutes
**Risk**: Medium - requires refactoring function signatures
**Test coverage**: Need new tests for column-level FKs

## Testing Plan

### Unit Tests
1. ✓ Created `test/parser_fk_debug_test.go` with 3 test functions
2. Run tests to confirm Fix 1 resolves table-level FK parsing
3. Add tests for Fix 2 to verify column-level FKs work

### Integration Tests
1. Update `test/integration/workflow_test.go` to use table-level FK syntax (already done)
2. After Fix 1: TestEndToEndWorkflow should pass
3. After Fix 2: Add test with column-level REFERENCES syntax

## Validation Criteria

### Success Metrics
- ✓ `TestRawPgQueryFKExtraction` passes
- ✓ `TestForeignKeyConstraintParsing` all 4 scenarios pass
- ✓ `TestColumnLevelFKParsing` extracts FK correctly
- ✓ `TestEndToEndWorkflow` passes (integration test)
- ✓ Generated DDL has correct FK column lists
- ✓ Migrations apply successfully to database

### Manual Verification
```sql
-- Parse this SQL and verify extracted FKs
CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    author_id INTEGER REFERENCES users(id),  -- Column-level
    CONSTRAINT posts_user_fkey FOREIGN KEY (user_id) REFERENCES users(id) -- Table-level
);

-- Expected:
-- FK 1: name="posts_user_fkey", cols=[user_id], ref=users(id)
-- FK 2: name=<generated>, cols=[author_id], ref=users(id)
```

## Prevention

### Why This Wasn't Caught Earlier
1. Parser tests focused on type parsing, not FK extraction
2. No dedicated FK parsing tests
3. Integration tests used inline REFERENCES syntax (which silently failed)
4. Topological sort masked the issue by reordering tables correctly

### Preventive Measures
1. ✓ Add comprehensive FK parsing test suite
2. Test both table-level and column-level FK syntax
3. Verify generated DDL actually applies to database
4. Add parser regression tests for all constraint types

## Timeline

| Task | Estimate | Status |
|------|----------|--------|
| Root cause analysis | 1 hour | ✓ Complete |
| Create focused test suite | 30 min | ✓ Complete |
| Fix 1: Use FkAttrs | 5 min | Pending |
| Test Fix 1 | 10 min | Pending |
| Fix 2: Column-level FKs | 45 min | Pending |
| Test Fix 2 | 15 min | Pending |
| Update integration tests | 15 min | Pending |
| Documentation | 15 min | Pending |
| **Total** | **2.5 hours** | **33% complete** |

## Related Issues

- Priority 1: Dependency Ordering (✓ Complete - tables now ordered correctly)
- Priority 2: Normalization Inconsistencies (Pending - separate issue)

## Conclusion

This is a **critical bug** that prevents foreign key constraints from working. The root cause is clear and the fix is straightforward:

1. **Immediate**: Change `constraint.Keys` to `constraint.FkAttrs` (5 minutes)
2. **Follow-up**: Support column-level REFERENCES syntax (45 minutes)

Once fixed, the dependency ordering implementation (Priority 1) will work end-to-end, and `TestEndToEndWorkflow` will pass.

---

**Prepared by**: Claude Code
**Reviewed by**: Pending
**Approved for implementation**: Pending

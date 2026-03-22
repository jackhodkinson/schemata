# Column Collation and Comment Metadata Dropped in DDL Generation

## Summary

Column-level `COLLATE` clauses and comments stored in `schema.Column.Collation` / `.Comment` are never emitted in generated DDL. When schemata recreates a table or adds a column, the collation and comment metadata disappear.

## Severity

**Medium** – functionally correct data definitions can still depend on explicit collations, and losing comments erodes documentation. Recreating objects via schemata may subtly change behavior.

## Reproduction

1. Define a column `email text COLLATE "en_US"` with a comment in `schema.sql`.
2. Run `schemata diff --from migrations`.
3. Generated DDL lacks the collation clause and the `COMMENT ON COLUMN` statement.

## Root Cause

- Parser/catalog populate `Column.Collation` and `Column.Comment`, but `internal/planner/ddl.go` ignores them when building column definitions.
- No follow-up `COMMENT ON` statements are emitted for columns.

## Fix Sketch

1. When constructing column definitions, append `COLLATE <collation>` if set.
2. After CREATE / ALTER statements, emit `COMMENT ON COLUMN schema.table.column IS ...` where comments exist.
3. Ensure differ detects comment changes (`comment changed`) and routes them to the comment generator rather than leaving TODO placeholders.

## Notes

We should adopt a consistent ordering: type → collation → default → generated/identity → NOT NULL. Comments may require separate statements because PostgreSQL’s grammar does not allow inline comments.

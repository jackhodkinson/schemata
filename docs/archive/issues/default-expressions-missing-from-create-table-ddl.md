# Default Expressions Missing from CREATE TABLE DDL

## Summary

Schemata tracks column defaults but drops them when generating `CREATE TABLE` statements. Freshly created tables therefore omit `DEFAULT` clauses even though the schema model knows about them.

## Severity

**High** – provisioning a new environment with schemata produces tables that lack expected defaults, potentially breaking application logic.

## Reproduction

1. Define `created_at timestamptz DEFAULT now()` in `schema.sql`.
2. Run `schemata diff` or generate a migration that recreates the table.
3. The `CREATE TABLE` DDL lacks the `DEFAULT now()` clause.

## Root Cause

- `internal/planner/ddl.go:170` builds column definitions for CREATE TABLE but only appends type, NOT NULL, and nothing else.
- ADD COLUMN DDL includes defaults, revealing the inconsistency.

## Fix Sketch

1. Update `generateCreate` to include defaults (and respect generated/identity ordering).
2. Ensure the parser/catalog loader normalize expressions consistently so comparison remains stable.
3. Add regression coverage ensuring a table definition with defaults round-trips.

## Notes

Once this is fixed, we should double-check that the DDL generator doesn’t emit redundant `SET DEFAULT` statements when the default is already present.

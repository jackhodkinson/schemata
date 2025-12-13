# Constraint Options Not Supported in Generated DDL

## Summary

Constraint metadata such as `DEFERRABLE`, `INITIALLY DEFERRED`, foreign-key `MATCH` modes, and `NOT VALID` flags are preserved in the schema model but omitted or simplified in generated DDL. As a result, constraints recreated by schemata can behave differently from the originals.

## Severity

**High** – applications depending on deferred constraints or specific match semantics will break if the constraint is recreated without those options.

## Reproduction

1. Create a deferred foreign key or an initially deferred check constraint in `schema.sql`.
2. Run `schemata diff` or recreate the table.
3. The output lacks `DEFERRABLE`, `INITIALLY DEFERRED`, and the foreign key’s `MATCH FULL/ SIMPLE`.

## Root Cause

- `schema.UniqueConstraint`, `CheckConstraint`, and `ForeignKey` structs carry these flags, but `internal/planner/ddl.go` ignores them when formatting constraint definitions.
- Alter-table flows only handle simple change types and fall back to TODO comments for nuanced option differences.

## Fix Sketch

1. Update constraint rendering helpers to append deferrability, match type, and validation clauses.
2. Teach alter-generation to drop/recreate constraints when relevant attributes change, ensuring the replacements include the full option set.
3. Expand differ tests to cover deferred and not-valid constraints.

## Notes

PostgreSQL defaults to `NOT DEFERRABLE INITIALLY IMMEDIATE`. Emitting only non-default clauses keeps DDL concise while preserving behavior.

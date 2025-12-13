# Index Metadata Not Round-Tripped

## Summary

Index definitions lose rich metadata—expressions, operator classes, collations, NULLS ordering, partial predicates—when schemata generates DDL. The schema model records these details, but the output flattens them to bare column lists.

## Severity

**High** – missing predicates or operator classes changes index semantics, potentially disabling partial indexes or specialized search paths.

## Reproduction

1. Define a partial index with `WHERE` clause or custom operator class in `schema.sql`.
2. Run `schemata diff` to preview the recreate.
3. Output lacks the predicate and opclass/collation annotations.

## Root Cause

- `schema.Index` tracks `KeyExprs`, `Predicate`, `Collation`, `OpClass`, etc., but `generateCreateIndex` (`internal/planner/ddl.go:230`) emits only simple expressions and drops opclasses, collations, and NULLS ordering.
- No handling for partial predicates beyond the bare expression string.

## Fix Sketch

1. Extend index rendering to include each key’s collation, opclass, ordering, and NULLS behavior.
2. Append `WHERE <predicate>` when defined.
3. Ensure the differ tests verify full fidelity for complex indexes.

## Notes

Follow PostgreSQL’s formatting conventions, e.g. `column_name COLLATE "en_US" opclass DESC NULLS LAST`. Keep output deterministic by preserving the order captured in `KeyExprs`.

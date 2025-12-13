# Table Reloptions and Comments Not Generated in DDL

## Summary

Tables in the schema model carry `RelOptions` (e.g. `fillfactor=90`) and `Comment`, but the DDL generator never includes them. Recreating a table via schemata silently drops storage parameters and descriptive metadata.

## Severity

**Medium** – losing reloptions changes performance characteristics, and missing comments erodes documentation. Applies to any `ALTER TABLE ... SET (fillfactor=...)` or `COMMENT ON TABLE`.

## Reproduction

1. Add `WITH (fillfactor=80)` and `COMMENT ON TABLE ...` to `schema.sql`.
2. Generate a migration or DDL preview.
3. Neither the reloptions nor the comment appear in the output.

## Root Cause

- `schema.Table.RelOptions` and `.Comment` are populated but unused in `generateCreate` and `generateAlterTable`.
- Differ emits `reloptions changed` / `comment changed`, but the planner falls back to TODOs because no handler exists.

## Fix Sketch

1. Extend CREATE TABLE DDL to append `WITH (<options>)` after the column list when `RelOptions` is non-empty.
2. After CREATE/ALTER flows, emit `ALTER TABLE ... SET (...)` and `COMMENT ON TABLE` statements as needed.
3. Add explicit handling for `reloptions changed` / `comment changed` in `generateAlterTable`.

## Notes

Options should be emitted in sorted order to keep diffs deterministic. Comments require separate `COMMENT ON TABLE` statements.

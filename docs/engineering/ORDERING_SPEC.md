# Deterministic Ordering Spec

This document defines ordering guarantees for `schemata` (object keys, diff output, and generated DDL).

## Goals

- Stable diff output regardless of map iteration order.
- Stable dependency graph queue ordering.
- A single shared key comparator used by differ/planner/CLI paths.
- Canonical ordering for `AlterOperation.Changes` and privilege DDL.

## Canonical Object Key Ordering

Object keys are sorted with this precedence:

1. `kind` (explicit rank order, not lexical)
2. `schema`
3. `name`
4. `table_name`
5. `column_name`
6. `signature`

### Kind Rank Order

1. `schema`
2. `extension`
3. `type`
4. `sequence`
5. `table`
6. `column`
7. `constraint`
8. `function`
9. `view`
10. `index`
11. `trigger`
12. `policy`
13. `grant`
14. `owner`

Unknown kinds sort after known kinds, then lexically by kind string.

## Where Object Key Ordering Is Enforced

- `pkg/schema/order.go` — `ObjectKeyLess`, `SortObjectKeys`
- `internal/differ/differ.go` — `Diff.ToCreate`, `Diff.ToDrop`, `Diff.ToAlter` sorted before return
- `internal/planner/graph.go` — topological queue tie-breaks use `ObjectKeyLess`

## Phase 2 — Generated DDL Internals (CREATE)

Where semantics allow, `internal/planner/ddl.go` emits stable ordering:

- **CREATE TABLE**: columns sorted by **column name**; `UNIQUE` / `CHECK` / `FOREIGN KEY` constraint blocks sorted by constraint **name** (ties broken by column-list key / expression text where unnamed).
- **CREATE INDEX** `INCLUDE (...)`: included columns sorted by name.
- **CREATE POLICY** `TO (...)`: role names sorted lexically.
- **CREATE TRIGGER**: event list sorted (e.g. `DELETE OR INSERT OR UPDATE`).
- **GenerateDDL** structural vs dependent key batches: each batch sorted with `SortObjectKeys` after the structural/dependent split.

**Not reordered** (would change meaning): composite PK/FK/UNIQUE **column lists**, function parameter order, **ENUM** label order.

## Phase 3 — AlterOperation.Changes Ordering

`AlterOperation.Changes` strings are sorted with `internal/differ.SortAlterChanges` using this **category precedence** (lower runs first), then lexical tie-break within a category:

1. **Owner** — `owner changed`
2. **Column structure** — `add column …`, `drop column …`
3. **Column alterations** — `alter column …: …`
4. **Primary key** — `add primary key`, `drop primary key`, `primary key … changed`, etc.
5. **Unique constraints** — add/drop/modify/validation lines
6. **Check constraints** — add/drop/modify/validation lines
7. **Foreign keys** — add/drop/modify/validation lines
8. **Table options** — `reloptions changed`
9. **Comment** — `comment changed`
10. **Privileges** — `add grant …`, `revoke grant …` (see Grant strings below)
11. **Fallback** — any unrecognized change string (lexical only)

Comparators emit changes in a stable order by sorting map keys before iteration; `SortAlterChanges` provides the final canonical order.

## Phase 3 — Grant and Revoke DDL Ordering

- If the **desired** schema object has **no grants** (empty `Grants`), the differ does **not** emit changes for privileges present only in the live database (same “no opinion” rule as unspecified `Owner`).
- **Grant records** on an object (`Table.Grants`, `View.Grants`, etc.) are compared after normalizing: sort by **grantee**, then **privileges** (lexical), then **grantable** flag.
- **Diff change strings**:
  - `add grant <grantee> <comma-separated-privileges> [grantable]` — privilege list sorted lexically; `grantable` suffix `true` / `false`.
  - `revoke grant <grantee> <comma-separated-privileges> [grantable]` — same shape for privileges being removed.
- **Emitted SQL**: `GRANT` / `REVOKE` statements sorted by grantee, then privilege list, then grantable, for deterministic output.

## Phase 3 — Owner DDL Placement

- **Table / view / function / sequence** owner changes emit `ALTER … OWNER TO …` when only ownership changes (or combined with other alters per planner rules).
- Owner-related `AlterOperation.Changes` entries sort before privilege changes (see category list above).

## Non-Reorder Exceptions (Global)

- Composite PK/FK/UQ **column lists** inside constraint definitions.
- Function **argument** order and defaults.
- **ENUM** label order.
- **Trigger** / **policy** semantics that depend on statement order when explicitly unsupported by sorting (planner should preserve dependency-driven order where required).

## Out of Scope / Follow-Ups

- `ALTER DEFAULT PRIVILEGES`, column-level ACL details beyond bundled grant rows.
- Broader statement-chunk ordering beyond the create/alter/drop pipeline when new object classes are added.

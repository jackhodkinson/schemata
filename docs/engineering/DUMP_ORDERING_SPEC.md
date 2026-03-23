# Dump Ordering Spec

This document defines ordering behavior for `schemata dump` in directory mode (one file per Postgres schema).

## Problem Statement

Per-schema file output is useful for ownership and review, but file name order is not dependency order.

Examples:

- `alpha.sql` may depend on objects in `zeta.sql` (cross-schema FK).
- `public.sql` may include extension setup while another schema uses extension-provided types.
- Views, triggers, functions, and custom types can introduce cross-schema ordering requirements.

Applying files lexicographically can therefore fail even when each individual file is valid.

## Reliability Model

`schemata` aims for Postgres-grade reliability by deriving object dependencies from the extracted object model and ordering schema files using a dependency projection.

This is more robust than pure name sorting, but not mathematically perfect for every possible SQL edge case.

## Current Ordering Contract

In directory dump mode:

1. Objects are grouped by owning schema.
2. Object-level dependencies are built via planner graph logic.
3. Cross-schema edges are projected into a schema dependency graph.
4. Schemas are topologically sorted.
5. Lexical ordering is used only as a deterministic tie-break for unrelated schemas.

If a dependency DAG cannot be formed (for example cycles), ordering falls back to deterministic lexical ordering.

## Dependency Sources Covered

- Table -> referenced table (foreign keys).
- View -> referenced objects from parsed view dependencies.
- Index -> owning table.
- Trigger -> owning table and trigger function.
- Function -> types in args/returns.
- Table -> custom/qualified column types.
- Table/function -> extension object heuristic for unqualified extension type names (for example `citext`).

## Known Limits

- Function body references are not fully parsed into dependency edges.
- Extension/type linkage for unqualified types uses heuristics and may miss unusual setups.
- Dependency cycles across schemas cannot be solved by linear ordering alone.
- Some canonicalization differences are PostgreSQL-driven (for example implicit/default index ordering display).

## Guidance for Users

- Prefer `schemata migrate` flow for production changes.
- If consuming split schema files directly, preserve generated order and validate in an ephemeral DB.
- If your stack has many cross-schema references, keep a single-file dump path for bootstrap/rebuild workflows.

## Test Expectations

Ordering tests should encode real object dependencies (not placeholder schema names only), so failures represent real ordering semantics rather than lexical assumptions.


# Normalization Contract

Schema equivalence normalization is centralized in `internal/normalize`.

## Purpose

Keep normalization behavior consistent across parser/catalog/differ/planner
flows by maintaining one canonical normalization contract.

## Canonical Entry Points

- `normalize.Object(schema.DatabaseObject) schema.DatabaseObject`
- `normalize.Expr(schema.Expr) schema.Expr`
- `normalize.FunctionBody(string) string`

`internal/differ` delegates to this module for hash normalization so any
behavior changes are localized and reviewable.

## Behavioral Guarantees

- Equivalent SQL expressions normalize to stable forms (e.g. `now()` and
  `CURRENT_TIMESTAMP`).
- Equivalent function bodies normalize while preserving quoted literals and
  quoted identifiers.
- Object-level normalization handles deterministic ordering and canonical
  representations for all supported object kinds.

## Contract Fixtures

`internal/normalize/normalizer_test.go` contains focused contract fixtures that
must remain green:

- expression cast/timestamp normalization
- function body canonicalization
- policy expression normalization

When adjusting normalization behavior, update fixtures intentionally and explain
why in the PR/commit.

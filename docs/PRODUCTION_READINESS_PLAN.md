# Production Readiness Plan (Postgres 15–18)

This document defines the path from the current state of `schemata` to a “swiss watch” open-source release that is safe for real production use.

## Target Support

- Postgres: **15, 16, 17, 18**
- Supported objects (v1 scope): schemas, extensions, types (enum/domain/composite), sequences, tables, columns, constraints (PK/UNIQUE/CHECK/FK), indexes, views/materialized views, functions, triggers, grants/ownership (where modeled).
- Policies: **nice-to-have** (optional; not required for v1).

## Guiding Principles

- **Fail closed**: never silently skip parse/capture errors that could change results.
- **No “TODO SQL”**: either generate correct DDL or stop with a precise error explaining what’s unsupported.
- **Safe by default**: avoid destructive or cascading operations unless explicitly enabled.
- **Version-insensitive diffs**: normalization must make equivalent schemas compare equal across PG 15–18.
- **Deterministic + testable**: a clean checkout can run unit tests, and CI can run integration tests against PG 15–18.

---

## Milestone v0.1 — Safe for Early Adopters (“never wrong silently”)

### Goals

- Stop producing partial/unsafe output.
- Make local development and CI deterministic for unit tests.

### Work Items

1. **Fail-closed parsing**
   - Change the parser to return errors (with statement location/snippet) when a statement cannot be interpreted as a tracked schema object and might affect correctness.
   - Current risk: silently skipping statements can omit objects and produce incorrect diffs/migrations.
   - Touchpoints: `internal/parser/parser.go`.

2. **Eliminate `-- TODO:` output paths**
   - Replace TODO placeholders in the DDL generator with structured “unsupported change” errors that include:
     - object key (`kind/schema/name[/table/signature]`)
     - change type
     - recommended manual remediation
   - Touchpoints: `internal/planner/ddl.go`.

3. **Remove default `CASCADE` on drops**
   - Default behavior should not be “drop dependents”.
   - Add an explicit opt-in flag (e.g. `--allow-cascade`) for destructive operations.
   - Touchpoints: `internal/planner/ddl.go`, CLI command wiring under `internal/cli/*`.

4. **Concurrency control**
   - Add a Postgres advisory lock around applying migrations to prevent concurrent runners racing.
   - Touchpoints: `internal/migration/applier.go`, potentially `internal/db/*`.

5. **Integration test determinism**
   - Ensure `go test ./...` passes in a clean environment by gating integration tests behind a build tag (e.g. `integration`) or auto-starting docker in the test harness.
   - Touchpoints: `test/integration/*`, `Makefile`, test README.

6. **Open-source hygiene baseline**
   - Add `LICENSE`.
   - Remove committed binaries from the repo (publish artifacts via releases instead).
   - Add CI for build + unit tests.
   - Touchpoints: repo root, `README.md`, `.github/workflows/*` (if/when added).

### Definition of Done

- Unit tests pass on a clean checkout via `go test ./...`.
- The CLI never emits `-- TODO:` placeholders in generated migrations/DDL.
- Default DDL generation avoids `CASCADE` unless explicitly enabled.
- Parser does not silently skip potentially relevant statements.

---

## Milestone v0.2 — Correct Diffs for Core Objects (“diff is trustworthy”)

### Goals

- Make `diff` stable and correct for real schemas, across PG 15–18.
- Eliminate known false positives and mismatches between parser vs catalog extraction.

### Work Items

1. **Fix index catalog extraction**
   - Stop using whole-statement strings as “key expressions”.
   - Extract index keys/expressions in a canonical form that matches parser output and remains stable across versions.
   - This is currently a known source of false positives (“key expressions changed”).
   - Touchpoints: `internal/db/catalog.go`, `internal/differ/*`, `test/integration/end_to_end_test.go`, plus unit tests.

2. **Fix function identity and DROP correctness**
   - Canonicalize function signatures for ObjectKey identity (types-only, stable formatting).
   - Generate correct `DROP FUNCTION schema.name(argtypes)` for overloaded functions.
   - Properly parse args/returns from catalog extraction (remove TODO).
   - Touchpoints: `internal/parser/parser.go`, `internal/cli/helpers.go`, `internal/db/catalog.go`, `internal/planner/ddl.go`.

3. **Version-aware catalog and normalization layer**
   - Detect `server_version_num`.
   - Normalize catalog text/expressions so equivalent schemas compare equal across PG 15–18.
   - Touchpoints: `internal/db/catalog.go`, `internal/db/normalize.go`, `internal/differ/hash.go`.

4. **Roundtrip invariants for each supported PG version**
   - For PG 15–18, prove:
     - apply schema.sql → extract objects → diff is empty
   - Add targeted fixtures for each object category (indexes, functions, triggers, views, constraints).
   - Touchpoints: `test/integration/*`, `docker-compose.yml`, CI.

### Definition of Done

- Integration suite passes against PG 15, 16, 17, and 18 in CI.
- End-to-end “roundtrip diff empty” holds for representative schemas containing the supported object set.
- Major known false positives are eliminated (especially indexes + functions).

---

## Milestone v1.0 — Production-Grade Apply (“apply is trustworthy under deploy constraints”)

### Goals

- Make migration application reliable in real deployment environments.
- Ensure generated migrations are safe, minimal, and operationally sane.

### Work Items

1. **Robust migration execution**
   - Support multi-statement migration files reliably.
   - Support non-transactional statements (e.g. `CREATE INDEX CONCURRENTLY`) via per-migration transaction controls.
   - Improve errors with file/version + statement index + snippet.
   - Touchpoints: `internal/migration/applier.go`, `internal/migration/scanner.go`.

2. **Dependency planning without relying on CASCADE**
   - Ensure create/drop order respects dependencies, including cross-object dependencies (views, functions, triggers, FK chains).
   - Provide understandable failures on cycles (already partially handled for create cycles).
   - Touchpoints: `internal/planner/graph.go`, `internal/planner/ddl.go`.

3. **Destructive-change safety model**
   - Default to “safe” operations; require explicit opt-in for drops/rewrites.
   - Add a “strict” mode that refuses destructive ops unless flags are present.
   - Provide a clear plan output that users can review (stable formatting).
   - Touchpoints: `internal/cli/*`, `internal/planner/*`.

4. **CLI correctness + ergonomics**
   - Avoid `os.Exit` from deep command logic; return errors for testability/composability.
   - Ensure consistent exit codes and machine-readable output options where appropriate.
   - Touchpoints: `internal/cli/diff.go` and other commands.

5. **Release engineering**
   - CI matrix: lint/vet, unit tests, integration tests for PG 15–18.
   - Reproducible builds; release artifacts published via GitHub Releases (not committed to git).
   - Touchpoints: `.github/workflows/*`, `README.md`.

### Definition of Done

- CI passes with PG 15–18 integration matrix.
- `schemata migrate` is safe-by-default, concurrency-safe, and supports real migration files.
- The tool either produces correct SQL or clearly refuses with actionable errors.

---

## Policies (Optional Track)

If implemented, policies should be:

- Clearly behind a feature flag or advertised as fully supported.
- Extracted/diffed with complete role handling (no placeholders).
- Tested across PG 15–18 in the same roundtrip model.


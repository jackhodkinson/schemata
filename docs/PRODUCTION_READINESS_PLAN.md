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

## Prioritized Remaining Work

The milestone sections below describe the intended capabilities. The following
list is the current priority order for reaching production readiness; release
version numbers alone should not be treated as evidence that a milestone is
complete.

### P0 — Correctness and Safety Blockers

1. **Establish an extensible architecture and code-quality baseline**
   - Define typed contracts for capture, normalization, identity, comparison,
     planning, rendering, and apply rather than relying on stringly-typed change
     descriptions and growing type switches.
   - Centralize identifier/literal quoting, capability registration, structured
     unsupported errors, and the supported-object matrix.
   - Keep package boundaries, dependency direction, tests, and developer
     tooling fast enough that new object types can be added without duplicating
     the full pipeline.

2. **Make normalization semantically safe**
   - Preserve quoted literals, identifier semantics, casts, column order,
     `search_path` order, and other meaning-bearing details.
   - Add explicit “must compare equal” and “must compare different” fixtures.

3. **Finish fail-closed parsing, extraction, and dump behavior**
   - Never substitute placeholder expressions or silently ignore unsupported
     schema-affecting statements, attributes, comments, grants, or owners.
   - Make `dump` fail instead of writing a partial schema after an object error.

4. **Make generated DDL correct or refuse it**
   - Fix function signatures and replacement behavior, enum evolution,
     identifier/literal quoting, index syntax, and dependency-aware create/drop
     ordering.
   - Use both desired and actual object maps when planning changes.
   - Complete end-to-end support for every advertised object, or explicitly
     reject and document objects that remain unsupported.

5. **Harden migration history and execution**
   - Record and verify migration checksums and metadata.
   - Reject duplicate versions and missing dependency references.
   - Missing-ledger bootstrap is fail-closed: normal execution and dry-run
     require explicit first-time initialization authorization under the runner
     lock, while recovery never creates history.
   - Per-migration transaction controls now support operations such as
     `CREATE INDEX CONCURRENTLY`, with durable statement progress, fail-closed
     interrupted states, and an explicit recovery workflow.
   - Report the failing file, statement index, and bounded SQL snippet.

6. **Add production safety controls**
   - Require explicit approval for destructive operations, table rewrites, and
     cascading changes.
   - Validate configuration after environment expansion and refuse empty or
     implicit target connections.
   - Cancellation plus finite, configurable PostgreSQL statement/lock timeouts
     are implemented and documented in [operational database safety](operational-safety.md).
   - Per-connection database/cluster identity checks are implemented; production
     targets must pin both values as documented in
     [operational database safety](operational-safety.md).
   - Add stable reviewable plan output.

7. **Resolve known security and release blockers**
   - Upgrade vulnerable dependencies and build with a supported, patched Go
     toolchain.
   - Add vulnerability scanning to CI and establish a dependency/toolchain
     update policy.

### P0.5 — High-Value Fuzzing and Property Tests

Add these while fixing the P0 contracts, rather than waiting until the end:

1. **Normalization properties**
   - Idempotence, safe equivalence transformations, and non-equivalence
     mutations for literals, casts, quoting, types, collations, and search paths.

2. **Parser fail-closed properties**
   - Arbitrary and mutated SQL must never panic, hang, emit placeholders, or
     silently lose schema-affecting information.

3. **Regression corpus**
   - Seed fuzzing from real `pg_dump` output, archived bugs, and production-like
     schemas; minimize every failure and commit it as a deterministic test.

### P1 — Cross-Version and Operational Confidence

1. **Postgres differential round trips**
   - Generate schemas, apply them to PG 15–18, extract them, and prove the diff
     is empty.
   - Generate schema evolutions, apply the planned DDL, and prove the resulting
     catalog matches the desired schema.

2. **Planner, DDL, migration graph, and configuration fuzzing**
   - Cover dependency cycles, overloaded functions, duplicate/missing
     migrations, quoted/Unicode identifiers, YAML/env expansion, and connection
     string boundaries.

3. **Replayable dump validation**
   - Restore every generated dump into an empty database and verify it with an
     empty diff in CI.

4. **Failure and concurrency testing**
   - Exercise cancellation, connection loss, concurrent runners, lock
     contention, commit ambiguity, server restart/failover, and partially failed
     non-transactional migrations.

5. **Production CI matrix**
   - Run unit, race, static analysis, vulnerability, and integration checks
     against every supported Postgres version.

### P2 — System-Critical Operational Hardening

1. **Operational documentation and recovery**
   - Document least-privilege roles, backups, forward-fix/rollback procedures,
     interrupted deployments, disaster recovery, and incident response.

2. **Observability and auditability**
   - Add structured/machine-readable output, migration timing and status,
     actionable lock diagnostics, and durable execution metadata.

3. **Performance and resource limits**
   - Test large catalogs and long migration histories, bound parser/catalog
     resource use, and simulate deploys under concurrent application traffic.

4. **Release supply chain**
   - Produce reproducible artifacts with SBOMs, provenance, and signatures, and
     verify them in the installation path.

5. **Long-running fuzz campaigns**
   - Run short deterministic fuzz smoke tests on pull requests, longer pure
     component fuzzing nightly, and scheduled stateful PG 15–18 campaigns.

### Production Exit Gate

Production readiness requires all P0 items, the P0.5 safety properties, and the
P1 cross-version/apply guarantees to be complete. For system-critical use, the
relevant P2 operational controls and runbooks must also be in place. “Fuzzing
ran for N hours” is not itself an exit criterion: discovered cases must become
deterministic regressions, and the defined safety properties must hold.

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

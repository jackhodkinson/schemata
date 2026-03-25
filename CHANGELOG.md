# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.4.0] - 2026-03-25

### Added

- `schemata fix extensions` command for onboarding existing migration directories. Detects extensions installed on the target database that are not created by any existing migration, and generates a bootstrap migration to create them. The generated migration is ordered before all existing migrations.
- Extensions documented as first-class schema objects in `schema.sql`. Schemata diffs, generates, and dumps extensions like any other object.
- `GenerateWithVersion` on the migration generator, allowing explicit version control for generated migrations.
- `ExtractExtensions` public method on `db.Catalog` for querying installed extensions directly.

## [v0.3.0] - 2026-03-23

### Added

- Moo-postgresql migration format support. Configure with `migrations: {dir: ./path, format: moo}` to read `.txt`/`.yml` files with `Description`/`Created`/`Depends`/`Apply` headers. Read-only — `generate` and `create` still produce native `.sql` files.
- Structured `migrations` config: supports both `migrations: ./path` (simple) and `migrations: {dir: ./path, format: moo}` (structured). Backward compatible.

## [v0.2.0] - 2026-03-23

### Added

- Migration dependency chains via `-- schemata:depends-on <version>` directives. Migrations are now topologically sorted respecting declared dependencies, with version-string ordering as a deterministic tie-breaker.
- `--step N` flag on `migrate` and `apply` commands to apply at most N pending migrations.
- `--to VERSION` flag on `migrate` and `apply` commands to apply up to and including a specific version.
- Per-schema directory dump mode: `schema` config can now point to a directory, emitting one `.sql` file per schema.
- Dependency-aware ordering for per-schema dumps (foreign keys, views, types, triggers).
- Comprehensive drift detection documentation for CI and operational monitoring.
- Distinct exit codes: exit 0 (in sync), exit 1 (drift found), exit 2 (runtime/config failure).

### Changed

- `Service.ApplyMigrations` now accepts `ApplyOptions` instead of a bare `dryRun bool`.

## [v0.1.0] - 2026-02-11

Initial release.

- Declarative schema management with `schema.sql` as source of truth.
- `schemata init`, `dump`, `generate`, `create`, `migrate`, `diff`, `apply`, `sync` commands.
- Multi-target database configuration via `schemata.yaml`.
- Environment variable interpolation in config (`${VAR}` and `${VAR:-default}`).
- CI drift detection with `schemata diff` and `schemata diff --from migrations`.
- Advisory-lock-based concurrency control for migration application.
- Cross-platform release builds (linux/darwin, amd64/arm64).

[v0.4.0]: https://github.com/jackhodkinson/schemata/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/jackhodkinson/schemata/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/jackhodkinson/schemata/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/jackhodkinson/schemata/releases/tag/v0.1.0

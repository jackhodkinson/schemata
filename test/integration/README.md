# Integration tests

The integration suite exercises parsing, catalog extraction, diffing, DDL,
migration execution, and CLI workflows against real PostgreSQL servers.

## Run locally

Docker and Go are required. PostgreSQL 16 is the default:

```sh
make test-integration
```

Set `POSTGRES_VERSION` to exercise another supported major release:

```sh
POSTGRES_VERSION=15 make test-integration
POSTGRES_VERSION=17 make test-integration
POSTGRES_VERSION=18 make test-integration
```

The compose project starts three isolated databases:

- development: `localhost:25433/schemata_dev`
- target: `localhost:25434/schemata_target`
- staging: `localhost:25435/schemata_staging`

Each PostgreSQL major uses separate named volumes. This prevents a local
cross-version run from trying to open another major release's data directory.
The compose configuration also uses the versioned `PGDATA` layout required by
the PostgreSQL 18 official image.

To keep the databases running while iterating:

```sh
POSTGRES_VERSION=16 make docker-up
go test -tags=integration -v ./test/integration/...
go test -tags=integration -v ./internal/cli/...
POSTGRES_VERSION=16 make docker-down
```

Run the two package groups sequentially because they intentionally share the
same databases and some CLI cases install or remove extensions.

The binary-level workflow can be run while the databases are up:

```sh
make build
make test-e2e
```

To verify that integration tests compile without starting PostgreSQL:

```sh
make test-integration-compile
```

## CI contract

CI runs the same tagged integration suite independently against PostgreSQL 15,
16, 17, and 18. A supported release may not be removed from that matrix without
an explicit support-policy change.

Integration failures must not be documented as expected false positives. Fix
the behavior or add a precise fail-closed rejection, then keep the case as a
deterministic regression test.

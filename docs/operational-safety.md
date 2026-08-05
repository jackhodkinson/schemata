# Operational database safety

Schemata bounds PostgreSQL work and responds to process termination requests by
default. These controls apply to catalog reads, drift checks, migration
pre-flight work, and migration execution, including fresh sessions opened for
individual migrations.

## Deterministic connection configuration

Schemata accepts only connection parameters that identify the endpoint,
credentials, database, and TLS configuration: `host`, `port`, `user`,
`password`, `dbname` (or `database`), `sslmode`, `sslrootcert`, `sslcert`, and
`sslkey`. Service files, password files, PostgreSQL runtime parameters, pgx pool
parameters, and unknown connection-string parameters are rejected. Put
connection, statement, and lock timeouts in Schemata's `database`
configuration rather than embedding runtime parameters in a connection string.
Each connection must identify exactly one server; multi-host URLs and host
lists are rejected. Use a reviewed proxy or load-balanced endpoint when the
database platform requires endpoint failover.

Every non-empty `PG*` environment variable understood by the pinned pgx
version is rejected when a connection is opened. This prevents ambient process
state from changing a connection that already passed Schemata's validation.
Environment placeholders remain supported, but use application-specific names
such as `${SCHEMATA_TARGET_URL}` or `${SCHEMATA_DB_PASSWORD}`; a placeholder
such as `${PGPASSWORD}` will still leave `PGPASSWORD` set and connection
creation will refuse it. Error messages identify conflicting variable names
but never include their values.

Schemata also prevents pgx from consulting `~/.pgpass` or automatically
discovered certificates under `~/.postgresql`. A password must therefore be
present in the explicit connection configuration when the server requires one,
and TLS file paths must be configured explicitly. Explicit `sslrootcert`,
`sslcert`, and `sslkey` values remain authoritative.

Treat the process environment as immutable after Schemata starts. pgx reads
process-global environment state while parsing a connection, and its public API
does not provide an environment-free parser. Schemata checks the complete pgx
environment surface immediately before parsing, but application code that
mutates `os.Environ` concurrently could race that check and is unsupported.

## First-time migration history initialization

Schemata never assumes that a missing `schemata.version` table means a fresh
database. It may instead mean that a previously managed database lost its
ledger, in which case replaying every migration could corrupt or destroy the
existing schema. Normal `apply` and `migrate` commands therefore refuse a
missing ledger before creating the reserved schema or executing migration SQL.
The same rule applies to dry-run.

For the first deployment to a verified empty target, explicitly authorize
ledger creation:

```shell
schemata migrate --target prod --initialize-history
# or, for the low-level command:
schemata apply --target prod --initialize-history
```

The decision is made while holding the ordinary migration-runner lock, so
concurrent initializers are serialized. `--dry-run --initialize-history`
reports that initialization would occur but creates neither the ledger nor any
other database object. Development workflows that apply migrations (`generate`,
and `diff --from migrations`) expose the same explicit flag rather than
silently opting in. `sync` always requires the flag because its documented
reset operation deliberately removes and recreates the dev ledger; validation,
reset, ledger recreation, and replay remain behind one runner lock. Schema
names read from the catalog are validated and quoted, and the complete schema
reset is one PostgreSQL transaction so any drop/recreate failure rolls it all
back. Applied-history source divergence is allowed because replacing edited or
deleted dev migrations is the purpose of this explicitly destructive command;
an incomplete `running` or `failed` history row must still be recovered or
resolved before reset.

Treat `--initialize-history` as a one-time bootstrap authorization, not a lost
history recovery switch. If a managed database is missing its ledger, stop the
deployment and restore the ledger from a trusted backup or incident-recovery
record. Do not authorize initialization merely to bypass the error. The
`recover` command intentionally never creates missing history.

## Required production target identity

Production targets must pin both the database name and PostgreSQL cluster
system identifier. Obtain them through an independently verified administrative
connection:

```sql
SELECT current_database(),
       (pg_catalog.pg_control_system()).system_identifier;
```

Then use the connection mapping form in `schemata.yaml`:

```yaml
target:
  url: ${TARGET_URL}
  identity:
    database: app_production
    system-identifier: "7561860200789946402"
```

The system identifier must be quoted and contain a decimal unsigned 64-bit
value. When an `identity` block is present, both fields are required. Schemata
rejects unknown keys in connection and identity mappings so a misspelling
cannot silently disable attestation. Both values are checked before Schemata
returns a database pool and whenever that pool opens a new physical session. A
mismatch or an inability to read the identity closes the connection and
prevents planning or execution against that target.

Scalar connection URLs remain supported for local and non-production
workflows, but they do not provide target attestation and production migration
commands reject them. Do not discover and accept identity values automatically
from the same untrusted runtime connection that will execute a migration; that
would make the check circular. Treat an identity change as an operational event
requiring independent verification and reviewed configuration updates.

Reading the system identifier calls `pg_catalog.pg_control_system()`. If a
provider or hardened cluster restricts it, a database administrator can grant
either the broader monitoring role or only the function privilege to the
Schemata login role:

```sql
GRANT pg_monitor TO schemata_login;
-- Least-privilege alternative:
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO schemata_login;
```

Prefer the function-level grant when the login does not otherwise need the
broader monitoring capabilities, and run that grant while connected to each
target database. Schemata includes equivalent role-quoted SQL in permission
errors.

## PostgreSQL timeouts

Every PostgreSQL connection or session opened by Schemata receives these
defaults:

- connection establishment: 15 seconds
- `statement_timeout`: 15 minutes
- `lock_timeout`: 10 seconds

Override any value in `schemata.yaml` with a Go-style duration:

```yaml
dev: ${DEV_URL}
target: ${TARGET_URL}

database:
  connect-timeout: 10s
  statement-timeout: 30m
  lock-timeout: 5s

schema: schema.sql
migrations: ./migrations
```

The connection timeout is applied independently to DNS resolution and to each
subsequent address connection attempt (TCP, TLS, and the PostgreSQL startup
exchange). It prevents either phase from hanging, but is not a whole-command
deadline. The statement timeout likewise applies independently to each
PostgreSQL statement. The lock timeout applies only while a statement is
waiting to acquire a PostgreSQL lock. A deployment that legitimately needs a
longer timeout should use an explicit, reviewed value appropriate to that
database.

`connect-timeout` must be greater than zero. An explicit `0` may disable
`statement-timeout` or `lock-timeout`; that escape hatch is not recommended for
unattended or production operation because it can leave a command or lock wait
unbounded. Negative and malformed durations are rejected while loading
configuration. PostgreSQL statement and lock timeout resolution is one
millisecond; smaller positive values round up to one millisecond.

## Cancellation and process signals

`SIGINT` (normally Ctrl-C) and `SIGTERM` cancel the active command context. The
cancellation reaches in-flight PostgreSQL operations, allowing pgx to request
query cancellation and transactional migration code to roll back. After the
first signal initiates graceful cancellation, normal signal handling is
restored so a second signal can force termination if a dependency does not stop
promptly.

Cancellation reports an error rather than success. Operators should inspect
the error and the migration ledger before retrying interrupted work; they
should never assume that a non-transactional database operation was reverted
solely because its client was canceled.

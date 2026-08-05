# Operational database safety

Schemata bounds PostgreSQL work and responds to process termination requests by
default. These controls apply to catalog reads, drift checks, migration
pre-flight work, and migration execution, including fresh sessions opened for
individual migrations.

## Required production target identity

Production targets should pin both the database name and PostgreSQL cluster
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

Scalar connection URLs remain supported for compatibility and local
development, but they do not provide target attestation. Do not discover and
accept identity values automatically from the same untrusted runtime connection
that will execute a migration; that would make the check circular. Treat an
identity change as an operational event requiring independent verification and
reviewed configuration updates.

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

Every PostgreSQL session opened by Schemata receives these defaults:

- `statement_timeout`: 15 minutes
- `lock_timeout`: 10 seconds

Override either value in `schemata.yaml` with a Go-style duration:

```yaml
dev: ${DEV_URL}
target: ${TARGET_URL}

database:
  statement-timeout: 30m
  lock-timeout: 5s

schema: schema.sql
migrations: ./migrations
```

The statement timeout applies independently to each PostgreSQL statement; it is
not a whole-command deadline. The lock timeout applies only while a statement
is waiting to acquire a PostgreSQL lock. A migration that legitimately needs a
longer operation should use an explicit, reviewed value appropriate to that
database.

An explicit `0` disables the corresponding PostgreSQL timeout. This escape
hatch is not recommended for unattended or production operation because it can
leave a command or lock wait unbounded. Negative and malformed durations are
rejected while loading configuration. PostgreSQL timeout resolution is one
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

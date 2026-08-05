# Operational database safety

Schemata bounds PostgreSQL work and responds to process termination requests by
default. These controls apply to catalog reads, drift checks, migration
pre-flight work, and migration execution, including fresh sessions opened for
individual migrations.

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

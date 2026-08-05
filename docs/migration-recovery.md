# Non-transactional migrations and recovery

Schemata runs migrations transactionally by default. Put this directive in the
leading comment block only when PostgreSQL requires a command to run outside a
transaction, for example `CREATE INDEX CONCURRENTLY`:

```sql
-- schemata:transaction off
CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email);
```

`-- schemata:transaction on` is accepted but optional. The directive is part of
the migration's exact checksummed source. Unknown, malformed, or duplicate
Schemata directives are rejected before database history is consulted.
Known session-local operations such as `SET`, prepared statements, temporary
objects, advisory locks, and transaction-controlling procedure calls are also
rejected in this mode because their effects cannot be reconstructed safely on a
fresh recovery session. Each top-level statement must be independently
resumable from its durable database effects.

Cluster-scoped commands such as database, tablespace, role/membership and
subscription management, `REASSIGN/DROP OWNED`, and `ALTER SYSTEM` are rejected
in every migration mode. Schemata's target identity, ledger, and advisory locks
are database-local and cannot safely coordinate those operations across a
PostgreSQL cluster.

## Execution model

A non-transactional migration is not atomic as a whole. Schemata therefore:

1. acquires the ordinary migration-runner lock, a control execution fence, and
   crosses an active-statement fence left by any earlier disconnected runner;
2. records a durable `running` history row;
3. executes each parsed top-level statement in its own newly opened physical
   session and implicit PostgreSQL transaction while holding the
   active-statement fence, then destroys that session;
4. durably increments `last_confirmed_statement` after every successful
   statement; and
5. marks the migration `applied` only after all statements are confirmed.

When PostgreSQL reports a statement error, Schemata records `failed`, the
one-based failing statement, a bounded error, and SQLSTATE when available.
Do not assume that means the catalog is unchanged: PostgreSQL explicitly
documents partial artifacts for some non-transactional operations (for example,
a failed `CREATE INDEX CONCURRENTLY` can leave an invalid index). Inspect and
clean up or complete those effects before retrying. A process crash or
connection loss can also happen after PostgreSQL commits a statement but before
Schemata confirms it in the ledger. That outcome is genuinely ambiguous, so the
row may remain `running`.

Normal `apply` and `migrate` runs refuse both `running` and `failed` history.
They never guess whether a statement committed and never resume partial work
automatically.

They also refuse when the complete `schemata.version` ledger is missing. A
missing ledger cannot be distinguished from a first-time database safely, so
normal execution and dry-run both require `--initialize-history` before they
will treat it as empty. Use that flag only for the first deployment to a
verified fresh database. If an existing deployment lost its ledger, restore
the ledger through the incident-recovery process instead; replaying migrations
from an invented empty history is unsafe. Recovery never initializes a missing
ledger.

The per-statement session boundary prevents settings, prepared statements,
temporary objects, roles, or advisory locks created indirectly by a routine or
trigger from leaking into later statements. Top-level `DO` and `CALL` are
rejected because their transaction and cluster effects cannot be inspected
safely. Migration SQL remains trusted executable code rather than a sandbox:
dynamic SQL inside arbitrary functions or triggers must still be constrained by
the deployment role's least-privilege permissions.

## Explicit recovery

First inspect the migration source, the target database, and the history row.
Do not edit the migration or the history table. Then choose exactly one action.

To retry, attest the number of leading statements that are already durable:

```shell
schemata recover VERSION --target prod --retry --confirmed-through N \
  --confirm-database DATABASE \
  --confirm-system-identifier SYSTEM_IDENTIFIER
```

`N` is a count: `0` means no statement is confirmed, `2` means statements 1
and 2 are confirmed, and execution resumes at statement 3. It cannot be lower
than the ledger's existing durable progress, and it must leave at least one
statement to run. If an ambiguous statement actually committed, either include
it in `N` after verifying its complete effect or manually restore a state in
which rerunning it is safe.

If manual inspection proves every statement already completed, attest that and
only update the ledger:

```shell
schemata recover VERSION --target prod --mark-applied \
  --confirm-database DATABASE \
  --confirm-system-identifier SYSTEM_IDENTIFIER
```

Recovery is refused unless the selected connection config pins both
`identity.database` and `identity.system-identifier`, and both values are
repeated on the command line. The connection layer then verifies those values
against the live PostgreSQL target before any ledger mutation. This binds an
operator attestation to a specific database in a specific cluster rather than
only to a convenient target alias.

`--mark-applied` does not execute or verify the migration's application-level
effects; the operator is asserting them. Both actions recheck the complete
local inventory, checksum, execution mode, statement count, dependencies,
tracking schema, and current history while holding the same locks used by a
normal deployment. Recovery also waits for any orphaned active-statement
backend to finish before changing the ledger. Use `--dev` instead of
`--target NAME` for the configured development database.

After recovery succeeds, run the normal schema diff and application health
checks before continuing the deployment.

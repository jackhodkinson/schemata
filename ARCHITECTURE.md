# schemata architecture

## Diff

When you run `schemata diff` it will compare the local schema file to your target db schema.

The way this works is by:
1. Parsing the schema file into (normalized) schema objects (schema, tables, columns, constraints, indexes, procedures, ...)
2. Querying the target to get these (normalized) objects
3. Identifying differences
4. If any generate DDL for the differences

### Normalization

Before comparing or hashing anything, both the local file and the target database are normalized into the same canonical form.
Normalization means:
•	schema-qualify all object and type names
•	lower-case unquoted identifiers
•	strip comments and redundant whitespace
•	order lists and options deterministically (e.g. columns, constraints, reloptions)
•	render defaults and expressions through pg_get_expr() or pg_get_*def() so their SQL text is stable
•	expand shorthand types like SERIAL into their explicit sequence + default form

This guarantees that equivalent schemas produce identical object JSON and hashes even if their raw SQL text or catalog formatting differ.

Constraint Names
	•	PK: <table>_pkey
	•	UNIQUE: <table>_<col...>_key
	•	FK: <table>_<col...>_fkey
	•	CHECK: name ignored (PG uses <table>_<n>_check)
Implicit Indexes
	•	Indexes backing PK/UNIQUE are implicit and not modeled as standalone indexes.

### Parsing objects from schema file
*produces a normalized map of desired objects*

We will use [libpg_query](https://github.com/pganalyze/libpg_query?tab=readme-ov-file) or one of the language-specific binding to parse the local sql file to build a model of the database schema.

We'll basically build a map with the following keys:
•	("schema", <name>)
•	("extension", <schema>, <name>)
•	("type", <schema>, <name>)          // enums, domains, composites
•	("sequence", <schema>, <name>)
•	("table", <schema>, <name>)
•	("column", <schema>, <table>, <name>)
•	("index", <schema>, <table>, <name>)
•	("constraint", <schema>, <table>, <name>)
•	("view", <schema>, <name>)          // includes matviews
•	("function", <schema>, <name>, <signature>) // signature canonicalized
•	("trigger", <schema>, <table>, <name>)
•	("policy", <schema>, <table>, <name>)
•	("grant", <kind>, <schema>, <name>, <grantee>, <priv-set>)
•	("owner", <kind>, <schema>, <name>)

And then objects (using json below for illustration purposes but we can define these types in whatever format is best suited for our desired program).

Table:
```json
{
  "owner": "app",
  "reloptions": ["fillfactor=90"],         // sorted
  "comment": "Users",
  "columns": [
    {"name":"id","type":"uuid","notnull":true,"default":"gen_random_uuid()","generated":null,"identity":null,"collation":null,"comment":null},
    {"name":"email","type":"citext","notnull":true,"default":null}
  ],
  "pkey": {"name":"users_pkey","cols":["id"]},
  "uniques":[{"name":"users_email_key","cols":["email"]}],
  "checks":[{"name":"users_status_chk","expr":"((status = ANY (ARRAY['active','disabled'])))"}],
  "fks":[{"name":"users_org_id_fkey","cols":["org_id"],"ref":{"schema":"org","table":"orgs","cols":["id"]},"on_update":"NO ACTION","on_delete":"CASCADE","match":"SIMPLE","deferrable":false,"initially_deferred":false}],
  "partition": null,                       // or spec
  "inherits": []
}
```


Index:
```json
{
  "table":"users",
  "unique":true,
  "method":"btree",
  "key_exprs":["email"],                   // expressions normalized
  "predicate":null,                        // partial index: normalized expr
  "include":[]                             // INCLUDE cols
}
```

Function:
```json
{
  "args":[{"mode":"in","name":"x","type":"int4"}], // normalized names/types
  "returns":"void",
  "language":"plpgsql",
  "volatility":"stable",                    // immutable|stable|volatile
  "strict":false, "security_definer":false,
  "search_path":[],                         // stripped from body
  "body":"BEGIN ... END",                   // normalized from pg_get_functiondef
  "parallel":"safe"
}
```

Enum:
```json
{ "values": ["draft","active","disabled"] } // order matters
```

Grants:
```json
{ "grantee":"app_reader", "privileges":["SELECT"], "grantable":false }
```

Views:
```json
{
  "schema": "sales",
  "name": "revenue_summary",
  "owner": "app_user",
  "type": "view",
  "security_barrier": false,
  "check_option": null,
  "comment": null,
  "definition": {
    "query": "SELECT c.id AS customer_id, c.name AS customer_name, SUM(o.total) AS total_revenue, COUNT(o.id) AS order_count FROM sales.customers c JOIN sales.orders o ON o.customer_id = c.id WHERE o.status = 'paid' GROUP BY c.id, c.name;",
    "dependencies": [
      { "kind": "table", "schema": "sales", "name": "customers" },
      { "kind": "table", "schema": "sales", "name": "orders" }
    ],
    "output_columns": [
      { "name": "customer_id", "type": "integer" },
      { "name": "customer_name", "type": "text" },
      { "name": "total_revenue", "type": "numeric" },
      { "name": "order_count", "type": "integer" }
    ]
  },
  "grants": [
    { "grantee": "public", "privileges": ["SELECT"], "grantable": false }
  ]
}
```

Etc.


### Extracting objects from target
*produces the same structure for actual objects*

We'll query the db to get the objects above from the following pg_catalog related tables: pg_namespace, pg_class, pg_attribute, pg_type, pg_constraint, pg_index, pg_proc, pg_trigger, pg_policy, pg_enum, pg_sequence, pg_extension, pg_depend

Then these will be parsed into the same objects as above.

### Diff algorithm

1.	build maps

•	desired = { key -> {hash, payload} } from schema.sql
•	actual  = { key -> {hash, payload} } from target db

all hashes are stable and order-independent. sorting keys *before* hashing is intentional.

2.	quick set math

•	to_create = desired.keys - actual.keys
•	to_drop   = actual.keys  - desired.keys
•	maybe_alter = desired.keys ∩ actual.keys where desired[key].hash != actual[key].hash

3.	only deep-compare the changed ones
for each key in maybe_alter, run a kind-specific comparer:

•	tables → compare cols/defaults/nullability/constraints/options
•	indexes → compare method/expr/predicate/include/unique
•	constraints → compare type (pk/uk/ck/fk) + details
•	views → compare normalized definition text
•	functions → compare signature-stable parts (volatility, language, body)
•	types/enums → compare values/order (enums: only allow ADD)
•	sequences → compare params/owned-by
•	triggers/policies → compare target + definition

that produces minimal ops like:
•	ALTER TABLE ... ADD COLUMN ...
•	ALTER TABLE ... ALTER COLUMN ...
•	DROP CONSTRAINT ... / ADD CONSTRAINT ...
•	CREATE OR REPLACE VIEW ...
•	CREATE INDEX ... then DROP INDEX ... (if replacing)
•	etc.

4.	optional: merkle speed-up

if you kept a Merkle tree (or per-schema hashes), first compare subtrees to skip loading/comparing unaffected groups. if a subtree hash matches, you don’t even iterate its leaves.

5.	plan order only if needed

•	if there are no differences, stop.
•	if differences exist but are local (e.g., add column on one table), you can emit immediately.
•	if any change crosses objects (views, FKs, triggers, functions, types), build the smallest dependency graph covering just those changed objects and their prerequisites, then:
•	create/alter in topo order,
•	drop in reverse topo.

tiny pseudocode:

```
desired = build_object_map(schema.sql)
actual  = build_object_map(target_db)

to_create = keys(desired) - keys(actual)
to_drop   = keys(actual)  - keys(desired)
to_check  = intersect(keys(desired), keys(actual))

to_alter = []
for k in to_check:
  if desired[k].hash != actual[k].hash:
    to_alter += diff_by_kind(desired[k].payload, actual[k].payload)

if empty(to_create) && empty(to_drop) && empty(to_alter):
  exit "in sync"

if needs_dependency_order(to_create, to_drop, to_alter):
  graph_file   = build_graph_for(desired, affected_objects)
  graph_target = build_graph_for(actual, affected_objects)
  plan = plan_with_graph(to_create, to_alter, to_drop, graph_file, graph_target)
else
  plan = simple_plan(to_create, to_alter, to_drop)

emit_sql(plan)
```

notes:
•	“affected_objects” = only objects touched by the changes + their prerequisites (not the whole DB).
•	cache {key -> hash} per target and for schema.sql so you can fast-exit when roots match.
•	rename detection (optional): when a drop+create look identical except for a name, treat as RENAME instead of drop/create.

### Planning and DDL generation

Once differences are identified we need to generate DDL which will show how to get from the target db's schema to the local schema (desired). In simple cases we can generate DDL statements for each diff independently. In more complicated cases this might require understanding the dependency graph to sort the DDL statements in the correct order. Dependency graphs are built only when cross-object operations are detected.


### Diff Summary

parse -> normalize -> hash -> diff -> plan (if needed) -> emit ddl


## Apply

`schemata apply <db>` is the lower-level command that applies pending migrations against the selected target or development database. Migration history is immutable: Schemata records the exact source checksum and refuses to run if applied history no longer matches the local inventory.

You must specify either a target or dev as the db.

### Migration state

```sql
CREATE SCHEMA IF NOT EXISTS schemata;

CREATE TABLE IF NOT EXISTS schemata.version (
  version_num text PRIMARY KEY,
  name text NOT NULL,
  checksum text NOT NULL,
  execution_mode text NOT NULL,
  status text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  statement_count integer NOT NULL,
  last_confirmed_statement integer NOT NULL,
  failed_statement integer,
  error_message text,
  error_code text,
  attempt_count integer NOT NULL,
  recovered_at timestamptz,
  recovery_action text
);
```

•	A migration’s version is the filename prefix: YYYYMMDDHHMMSS-...sql → version_num = YYYYMMDDHHMMSS.
•	The checksum is SHA-256 over the exact source-file bytes, including comments, directives, and line endings. Moo migrations hash the complete source file rather than only the extracted Apply block.
•	Before planning pending work, Schemata rejects changed checksums, renamed migrations, missing local versions, changed transaction modes or statement counts, incomplete states, and applied migrations whose dependencies are not applied.
•	The progress and recovery columns form the durable state model for non-transactional execution. Transactional migrations are recorded as applied atomically with their schema changes. Non-transactional migrations confirm each committed top-level statement separately and require explicit operator recovery after any incomplete attempt.

How apply runs
1.	Resolve the target, load and validate every migration, calculate exact checksums, split top-level statements, and reject explicit transaction-control commands.
2.	Create and validate the internal tracking schema.
3.	Acquire one session advisory lock on a dedicated connection for the complete history-check and execution lifecycle.
4.	Validate the complete database history against the complete local inventory.
5.	Compute and dependency-sort pending migrations, then apply requested step/version filters.
6.	For each pending transactional migration:
   - Open a fresh execution session, begin a transaction, and acquire a transaction-scoped execution fence.
   - Recheck the exact history and insert a `running` row inside that transaction.
   - Execute each top-level statement in order, reporting the exact failing statement if one errors (unless `--dry-run`).
   - Resolve deferred constraints, then prove that the tracking schema and existing history were not changed by the migration.
   - Update the row to `applied` and verify the transition.
   - Commit. On error: roll back the migration and stop.
   - Destroy the execution session so migration-created session state cannot leak into another migration or the pool.
7.	For each pending migration whose leading header declares `-- schemata:transaction off`:
   - Acquire a session-scoped control fence and cross a separate active-statement fence. This allows statements such as `CREATE INDEX CONCURRENTLY` while preventing recovery from overlapping an orphan executor.
   - Insert a durable `running` row. Execute each top-level statement on a newly opened physical session, holding the active-statement fence for its full lifetime, then destroy that session and durably advance `last_confirmed_statement`.
   - On a definite PostgreSQL statement error, persist `failed`, the one-based statement index, a bounded diagnostic, and SQLSTATE when available. A crash or connection loss in the commit-to-confirmation window may intentionally leave `running` because its outcome is ambiguous.
   - Mark the migration `applied` only after every statement is confirmed. Never resume an incomplete row implicitly.
8.	Release the runner lock and destroy its dedicated session.

Dry-run does not create the tracking schema on a fresh database.

Incomplete non-transactional migrations are reconciled with the explicit `schemata recover` workflow described in [migration recovery](docs/migration-recovery.md). Retry requires an operator-attested `--confirmed-through N` boundary; `--mark-applied` attests that all statements are already durable.

Migration files are trusted executable code, not sandboxed input. Schemata rejects direct tracking-schema access and verifies the durable ledger around execution, but a deployment role with unrestricted privileges can deliberately bypass in-process safeguards. Production deployments must therefore use the documented least-privilege role boundary once that operational work is complete.


## Migrate

When the user runs `schemata migrate` it will do the same as apply but also first ensures that `schemta diff --from migrations` is clean. This ensures that the migrations directory matches the local `schema.sql` file.

If the migrations directory is out of sync with the schema file then a nice error will raise to warn the user and tell them to run `generate` to ensure their migrations directory is in sync with the local schema.sql file.

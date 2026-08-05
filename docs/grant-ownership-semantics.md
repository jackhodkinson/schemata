# Grant and ownership semantics

Schemata models ownership and object-level ACLs for tables, sequences, regular
and materialized views, functions (including exact overload identities), enums,
domains, and composite types. PostgreSQL roles must already exist; creating and
managing roles is intentionally outside this feature.

Ownership and grants are independently optional in the desired schema:

- `Owner == nil` means ownership is unmanaged. A non-nil owner requires that
  exact role.
- `Grants == nil` means the ACL is unmanaged. A non-nil empty grant list means
  the object must have no explicit ACL entries. A non-empty list is the exact,
  authoritative ACL.

PostgreSQL supplies default privileges even when an ACL catalog column is null.
Catalog extraction expands those defaults into explicit grants. When an ACL is
managed, generated DDL first revokes all privileges from `PUBLIC` and the object
owner, then emits the declared grants in deterministic order. Ownership still
gives the owner its inherent PostgreSQL capabilities; an empty ACL means no ACL
entries, not that the owner loses ownership powers. Catalog capture preserves an
explicitly empty ACL as managed empty state so dump and replay cannot restore
PostgreSQL defaults accidentally.

When a structural view change requires replacement, an unmanaged owner or ACL
is copied from the observed view to the replacement. Replacement fails closed
if the prior view metadata is unavailable; `nil` never means “replace this with
the migration user's defaults.”

PostgreSQL requires a sequence linked with `OWNED BY` to have the same owner as
its table. Schemata validates that invariant before rendering, creates the base
sequence before any table default that references it, and defers the `OWNED BY`
binding until the target column exists. During a joint owner transition, the
table owner changes first (PostgreSQL cascades that owner to the linked
sequence), after which any managed sequence ACL is rebuilt exactly.

The `PUBLIC` pseudo-role and an ordinary role literally named `"PUBLIC"` are
different typed grantees. Schemata preserves that distinction and always quotes
ordinary role names.

The following shapes fail closed instead of being silently omitted:

- column-level ACLs;
- regular-view and materialized-view column comments, regular-view column
  defaults, and materialized-view per-column statistics, storage, compression,
  or attribute options;
- explicit `GRANTED BY`, or catalog ACL entries whose grantor is not the object
  owner;
- ambiguous or unattached grant, revoke, comment, or owner statements;
- grants over unmodeled object families or bulk targets such as `ALL TABLES IN
  SCHEMA`;
- function grants or ownership changes without an exact identity signature;
- comments or non-default ACLs attached to an implicit identity backing
  sequence, because those fields are not represented on an identity column.

`MAINTAIN` is modeled and can be declared explicitly when targeting PostgreSQL
17 or newer. `GRANT ALL` on tables and views is rejected because `ALL` gained
`MAINTAIN` in PostgreSQL 17 and therefore has version-dependent meaning; list
the privileges explicitly. `REVOKE ALL` remains supported as a version-neutral
way to declare a managed empty ACL.

## Capability matrix

The following object families have parser input, catalog capture, replayable
create/dump DDL, and owner/comment/object-level ACL metadata covered by the live
metadata round-trip:

| Object family | SQL parser | Catalog capture | Create/dump | Metadata identity |
| --- | --- | --- | --- | --- |
| Table | Yes | Yes | Yes | Qualified table name |
| Regular view | Supported subset | Yes | Yes | Qualified view name |
| Materialized view | Supported subset | Supported subset | Yes | Qualified materialized-view name |
| Sequence | Supported subset | Supported subset | Yes | Qualified sequence name |
| Function | Yes | Yes | Yes | Qualified name plus input argument types |
| Enum | Yes | Yes | Yes | Qualified type name |
| Domain | Yes | Yes | Yes | Qualified type name |
| Composite type | Supported subset | Supported subset | Yes | Qualified type name |

“Supported subset” is deliberate and fail-closed. Materialized-view storage
options, tablespaces, non-heap access methods, and `WITH NO DATA`; temporary or
unlogged sequences; regular-view options/output aliases/check options; and
composite-attribute collations are rejected because the current schema model
cannot replay them exactly. This includes view-column metadata and materialized
view per-column tuning listed above. Structural domain and composite alterations
also require an explicit migration; metadata-only owner/comment/ACL alterations
are generated in place.

Catalog normalization collapses a SERIAL backing sequence only when its name,
type, options, owner, default ACL, dependency, and exact `nextval(regclass)`
reference match PostgreSQL's implicit SERIAL shape. Customized sequences remain
explicit objects. Identity sequence names and options are preserved on the
column, including descending-sequence defaults. Adding/removing an identity,
renaming or restarting its sequence, or changing its sequence options requires
an explicit migration; only `GENERATED ALWAYS` ↔ `BY DEFAULT` is altered in
place automatically.

# ENUM Default Value Detection Bug

## Summary

Schema comparison incorrectly detects a difference in default values for columns with custom ENUM types, even when the defaults match. This causes the pre-flight check to fail indefinitely, blocking migration application.

## Severity

**Critical** - Blocks the ability to apply migrations that add ENUM columns with default values.

## Environment

- PostgreSQL version: 17.x (tested on local instance)
- Schemata: latest (as of 2025-10-20)

## Steps to Reproduce

1. Create a custom ENUM type and add a column using it with a default value to schema.sql:

```sql
CREATE TYPE user_role AS ENUM ('guest', 'user', 'moderator', 'admin');

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    role user_role DEFAULT 'user' NOT NULL
);
```

2. Generate a migration:
```bash
schemata generate 'add user role enum'
```

3. The generated migration correctly includes:
```sql
CREATE TYPE public.user_role AS ENUM ('guest', 'user', 'moderator', 'admin');
ALTER TABLE public.users ADD COLUMN role user_role NOT NULL DEFAULT 'user';
```

4. Sync the dev database:
```bash
schemata sync
```

5. Check for differences:
```bash
schemata diff --from migrations
```

## Expected Behavior

`schemata diff --from migrations` should report no differences, as the dev database schema now matches the schema.sql file.

## Actual Behavior

`schemata diff --from migrations` incorrectly reports:

```
Schema differences found between schema.sql and dev:

Objects to ALTER (1):
  ~ table: public.users
      alter column role: default changed

DDL Preview:
---
ALTER TABLE public.users ALTER COLUMN role SET DEFAULT 'user';
---
```

This causes `schemata migrate` to fail with:
```
Error: pre-flight check failed: migrations are out of sync with schema.sql:
  0 to create, 0 to drop, 1 to alter
```

## Impact

- Cannot apply migrations that add ENUM columns with default values
- The pre-flight check creates an infinite loop
- No command-line flag exists to bypass the pre-flight check
- Workarounds (generating additional migrations, splitting operations, explicit type casting) all fail

## Attempted Workarounds

All of the following were attempted and failed to resolve the issue:

1. **Generated a second migration** to explicitly set the default - same issue persists
2. **Split ADD COLUMN into multiple statements**:
   ```sql
   ALTER TABLE public.users ADD COLUMN role user_role;
   ALTER TABLE public.users ALTER COLUMN role SET DEFAULT 'user';
   ALTER TABLE public.users ALTER COLUMN role SET NOT NULL;
   ```
3. **Used explicit type casting**:
   ```sql
   ALTER TABLE public.users ALTER COLUMN role SET DEFAULT 'user'::user_role;
   ```

## Root Cause Hypothesis

The issue appears to be in the schema comparison logic for ENUM default values. There's likely a mismatch between:
- How PostgreSQL stores/represents default values for custom ENUM types internally
- How schemata parses default values from the schema.sql file

This could be related to:
- Type coercion representation (`'user'` vs `'user'::user_role`)
- Quote handling in default expressions
- Schema vs catalog representation differences

## Suggested Fix Areas

1. **Schema parser** (`pkg/parser` or similar): Ensure ENUM default values are parsed consistently
2. **Schema comparison** (`pkg/diff` or similar): Normalize ENUM default value representations before comparison
3. **Query logic** for extracting defaults from PostgreSQL catalog tables - may need to handle ENUM types specially

## Reproduction Repository

Test case available in: `/Users/jackhodkinson/code/test-schemata`

Migration file: `migrations/20251020153610-add-user-role-enum-and-role-column.sql`

## Additional Context

This was discovered during systematic testing of schemata's handling of more complex PostgreSQL features. The basic ENUM type creation and column addition work correctly - only the default value comparison is broken.

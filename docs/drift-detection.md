# Drift Detection

Use `schemata diff` to detect schema drift across your development workflow and runtime environments.

## Ways to use drift detection

`schemata` supports two core drift checks:

- `schemata diff`: compares your configured target database to `schema.sql`.
- `schemata diff --from migrations`: applies migrations to `dev`, then compares that result to `schema.sql`.

### 1) Pull request gate (migrations -> desired schema)

Use `schemata diff --from migrations` in pull request CI when your goal is to validate that committed migrations reproduce the committed desired schema.

This pattern does not require each developer to manually apply migrations before merging. CI performs the migration replay check centrally.

Because `--from migrations` applies migrations, point `dev` to an ephemeral CI database rather than a shared long-lived development database.

### 2) Environment drift monitoring (live DB -> desired schema)

Use plain `schemata diff --target <env>` for scheduled or post-deploy checks against staging/production.

This catches out-of-band changes made directly in an environment, for example:

- A table is created manually in production.
- An index is dropped during incident response and not codified in migrations.
- A role, grant, or function is changed directly in a live database.

A pull request gate using `--from migrations` will not catch these changes by itself. It validates repository consistency, not live environment state over time.

## Recommended baseline

Keep these files in the repository:

- `schemata.yaml`
- `schema.sql` (or your configured schema file)
- migrations directory (for example `./migrations`)

Prefer environment-variable driven connection configuration in `schemata.yaml`:

```yaml
dev: ${DEV_URL}
target: ${TARGET_URL}
schema: schema.sql
migrations: ./migrations
```

For multi-target environments:

```yaml
dev: ${DEV_URL}
targets:
  prod: ${PROD_URL}
  staging: ${STAGING_URL}
schema: schema.sql
migrations: ./migrations
```

## Commands

Compare your configured target against the schema file:

```bash
schemata diff --config schemata.yaml
```

For multi-target configs, select the target explicitly:

```bash
schemata diff --config schemata.yaml --target prod
```

Compare migrations (applied to dev) against the schema file:

```bash
schemata diff --config schemata.yaml --from migrations
```

## Schema directory mode and ordering

If `schema` is configured as a directory path, `schemata` reads multiple `.sql` files as one desired schema.

- This improves ownership and review for large schemas.
- Split files can still have cross-schema dependencies (FK/view/trigger/type/extension).
- `schemata dump` in directory mode emits files in dependency-aware order with deterministic tie-breaks.

Important:

- Dependency-aware ordering significantly reduces apply-order failures, but does not guarantee perfect handling for every possible SQL edge case.
- Keep an ephemeral DB replay check in CI for workflows that execute split files directly.
- For bootstrap workflows where strict ordering is critical, single-file schema mode remains the most conservative option.

## Exit behavior

- Exit `0`: schemas are in sync.
- Exit `1`: drift was found.
- Exit `2`: runtime/config/connectivity failure occurred.

## GitHub Actions example (pull request gate)

```yaml
name: schema-drift

on:
  pull_request:
  push:
    branches: [main]

jobs:
  drift-check:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_DB: schemata_ci
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U postgres -d schemata_ci"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 12
    steps:
      - uses: actions/checkout@v4

      - name: Install schemata
        run: |
          mkdir -p "$HOME/.local/bin"
          curl -fsSL https://raw.githubusercontent.com/jackhodkinson/schemata/main/install.sh | VERSION=v0.1.0 INSTALL_DIR="$HOME/.local/bin" sh
          echo "$HOME/.local/bin" >> "$GITHUB_PATH"

      - name: Check migration-to-schema drift
        env:
          DEV_URL: postgres://postgres:postgres@localhost:5432/schemata_ci?sslmode=disable
        run: schemata diff --config schemata.yaml --from migrations
```

## GitHub Actions example (scheduled environment monitoring)

```yaml
name: schema-drift-prod

on:
  schedule:
    - cron: "0 * * * *"
  workflow_dispatch:

jobs:
  drift-check-prod:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install schemata
        run: |
          mkdir -p "$HOME/.local/bin"
          curl -fsSL https://raw.githubusercontent.com/jackhodkinson/schemata/main/install.sh | VERSION=v0.1.0 INSTALL_DIR="$HOME/.local/bin" sh
          echo "$HOME/.local/bin" >> "$GITHUB_PATH"

      - name: Check production environment drift
        env:
          PROD_URL: ${{ secrets.PROD_URL }}
        run: schemata diff --config schemata.yaml --target prod
```

## Common pitfalls

- Multiple targets configured but no `--target` provided.
- Missing environment variable interpolation (for example `${PROD_URL}` not set).
- DB auth or SSL settings differ between local and CI.
- Wrong `--config` path from CI working directory.
- `--from migrations` without a valid dev database connection.
- Running `--from migrations` against a shared dev database can produce noisy or unsafe CI behavior.
- Migration replay fails with `type "citext" does not exist` or similar — your existing migrations assume extensions that they never create. Run `schemata fix extensions` to generate a bootstrap migration.

## Local repro command

When CI fails, run the same command locally with verbose output:

```bash
schemata diff --config schemata.yaml --from migrations --verbose
```

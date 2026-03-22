# Schemata

A declarative postgres migration tool

## Installation

### Quick install (curl)

```bash
curl -fsSL https://raw.githubusercontent.com/jackhodkinson/schemata/main/install.sh | sh
```

Install a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/jackhodkinson/schemata/main/install.sh | VERSION=v0.1.0 sh
```

By default this installs to `~/.local/bin/schemata`. Override with `INSTALL_DIR=/usr/local/bin`.

### Go install

```bash
go install github.com/jackhodkinson/schemata/cmd/schemata@latest
```

### Manual download

Download prebuilt binaries and checksums from [GitHub Releases](https://github.com/jackhodkinson/schemata/releases).

## Capabilities

- Define your schema in raw SQL
- Generate migrations from changes to your schema
- Configure `schemata` with a config.yaml file
- Dump an existing database with optional filtering rules

## Commands

If you are starting from an existing db without any local schema file or migrations directory you can do this:

```bash
schemata init
schemata dump
schemata generate 'my first migration'
# modify the new migration file and apply the migration
schemata migrate
```

Then going forward you can modify `schema.sql` (or whatever schema file you specify in `init`) and generate a migration via `schemata generate` and apply it via `schemata migrate`.

To apply a manual migration you will run:

```bash
schemata create 'my manual migration'
# edit the new migration file and apply to target
schemata migrate
```

If you are starting from an existing db with a local migrations directory it will be automatically detected if they live in `./migration` or if they reside in a different directory you can specify the path to their directory using `--migrations`

```bash
schemata init --migrations ./db/migrations/
schemata dump
# Check if the migrations are in sync with your target
schemata diff
# Optionally if they are not in sync you can create a new baseline migration
# schemata generate 'my baseline migration'
```

Or if you are starting with both a migrations and a local schema file you won't need to dump:

```bash
schemata init --migrations ./db/migrations/ --schema ./db/schema.sql
schemata diff
```

### Generate

The `schemata generate` command will read the `schema` path and compare it to the dev db.


### Initialization

*note to agents:* Don't worry about implementing the interactive init for now. Just focus on the `init` command with the required flags.

```bash
# initialize with interactive settup
schemata init

# or init in one liner
schemata init \
  --dev $DEV_URL \
  --target $TARGET_URL \
  --migrations ./migrations \
  --config schemata.yaml \
  --schema schema.sql
```

This will generate a yaml file like:

```yaml
# schemata.yaml
dev: ${DEV_URL}
target: ${TARGET_URL}
schema: schema.sql
migrations: ./migrations
```

Note we have auto detected that $DEV_URL and $TARGET_URL are environment variables and saved them in the config as environment variables.

The user can also specify multiple targets via `--target prod=$PROD_URL --target staging=$STAGING_URL` which will generate a yaml like this:

```yaml
# schemata.yaml
dev: ${DEV_URL}
targets:
  prod: ${PROD_URL}
  staging: ${STAGING_URL}
schema: schema.sql
migrations: ./migrations
```

Then the user must specify `--target` when running `apply` or `diff` commands.

```bash
schemata migrate --target staging
```

### Dump

The `schemata dump` command will dump the target db schema to a `sql` file whos path is specified in `config.schema` or `config.schema.file`.

Optionally the user can override the config file or use the `dump` command without a config file if they specify `--schema` while running the command.

If neither a `schemata.yaml` file or a `--schema` flag is passed then the command will show a nice error.


## Configuration

### DB connections

You specify the target database(s) you want to interact with in the `schemata.yaml` file or during the `init` process.

You also need a dev database connection to allow us to diff your migrations directory against the desired schema.

The simplest approach is to have a single target and dev connection:

```yaml
# schemata.yaml
dev: ${DEV_URL}
target: ${TARGET_URL}
```

Note that environment variables are parsed in as default when you use the syntax `${...}`

Optionally you can include multiple target dbs:

```yaml
# schemata.yaml
dev: ${DEV_URL}
targets:
  prod: ${PROD_URL}
  staging: ${STAGING_URL}
  dev: ${DEV_URL}
```

You can also break down the URL into host/port/username/password/database/ssl

```yaml
dev: ${DEV_URL}
target:
  host: ${TARGET_HOST}
  port: ${TARGET_PORT}
  username: ${TARGET_USERNAME}
  password: ${TARGET_PASSWORD}
  database: ${TARGET_DATABASE}
  ssl:
    mode: require  # or: disable, allow, prefer, require, verify-ca, verify-full
    ca-cert: /path/to/ca-certificate.crt
    client-cert: /path/to/client-certificate.crt
    client-key: /path/to/client-key.key
```

All of these fields are optional. If you omit them, `schemata` leaves those connection settings unset and PostgreSQL/libpq fallback behavior applies (for example, `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`):

```yaml
target:
  host: ${TARGET_HOST}
  # port: ${TARGET_PORT}
  # username: ${TARGET_USERNAME}
  # password: ${TARGET_HOST}
  # database: ${TARGET_DATABASE}
```

However, if you specify an empty/null value then this will be treated as empty/null and may error

```yaml
target:
  host: ${TARGET_HOST}
  port:
  # username: ${TARGET_USERNAME}
  # password: ${TARGET_HOST}
  # database: ${TARGET_DATABASE}
```

This can be done for the dev db and multiple targets as well:

```yaml
dev:
  host: ${DEV_HOST}
targets:
  prod:
    host: ${PROD_HOST}
    port: 5432
    username: ${PROD_USERNAME}
    password: ${PROD_PASSWORD}
    database: myapp_production
    ssl:
      mode: verify-full
      ca-cert: ${HOME}/.postgresql/prod-ca.crt

  staging:
    host: ${STAGING_HOST}
    port: 5432
    username: ${STAGING_USERNAME}
    password: ${STAGING_PASSWORD}
    database: myapp_staging
    ssl:
      mode: require

  local:
    host: localhost
    port: 5433
    username: postgres
    database: myapp_local
    # No SSL for local development
```

### Schema filtering

When working with postgres db with multiple schema you can choose to include/exclude schema

```yaml
# schemata.yaml
schema:
  file: schema.sql
  include:
    - sales
    - finance
    - product
```

or exclusive

```yaml
# schemata.yaml
schema:
  file: schema.sql
  exclude:
    - finance
    - sales
```


## Migrations

## File format

When you run `schemata generate <migration-name>` or `schemata create <migration-name>` it will add a new migration file into your migrations directory (as specified in `schemata.yaml`).

The `<migration-name>` variable must be close enough to file-name safe. We'll take it and make it kebab case for the file name and prepend a timestamp onto the file path so it becomes `${timestamp}-${fileSafe(migrationName)}`.

## Generating migrations

When you generate a migration using `schemata generate` it will make sure all your existing migrations are applied to the dev db, and then diff this with your local schema file. The diff will be used to generate a new migration file.

## Manual migrations

You can manually create a migration using `schemata create` which will place an empty migration file in your migrations directory.

## Applying migrations

When you run `schemata migrate` it will apply all unnaplied migrations to your target.

## Migration state tracking

When you run `schemata migrate` for the first time it will create a new schema in your target db called `schemata.migration_version` with the following table:

```sql
CREATE SCHEMA IF NOT EXISTS schemata;

CREATE TABLE IF NOT EXISTS schemata.version (
  version_num text PRIMARY KEY
);
```

The version field stores the timestamp prefix (e.g., '20241015120530'), while name stores the kebab-case migration name.


## Diff

When you run `schemata diff` it compares the local schema file against the target db schema.

You can also run `schemata diff --from migrations` which will apply your migrations to the dev db and calculate a diff from the dev db to the local schema file.

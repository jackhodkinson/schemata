# pg_query_go macOS 15.4+ Workaround

macOS 15.4 (a.k.a. Sequoia) ships an implementation of `strchrnul` in the
system SDK. The PostgreSQL port of `snprintf` bundled inside
`github.com/pganalyze/pg_query_go/v5` also defines a static fallback with the
same symbol name. When we build schemata with CGO enabled, Clang sees the
system declaration first, then the fallback, and errors with:

```
static declaration of 'strchrnul' follows non-static declaration
```

PostgreSQL fixed this upstream by renaming the fallback and only defining it
when the system declaration is unavailable. The change has not been released in
pg_query_go yet, so we vendor a patched copy locally.

## What We Changed

- `third_party/pg_query_go/v5/parser/src_port_snprintf.c` now matches the
  upstream PostgreSQL logic: it wraps the fallback in `#if !HAVE_DECL_STRCHRNUL`
  and aliases it to `pg_strchrnul` so the name never collides with the SDK
  symbol.
- `third_party/pg_query_go/v5/parser/include/postgres/pg_config.h` defines
  `HAVE_DECL_STRCHRNUL` to `0` by default to keep the guard predictable across
  platforms.
- `go.mod` contains a `replace` directive that points pg_query_go to that
  patched directory.

The rest of the module is untouched-it is the stock `v5.1.0` release.

## How To Verify

1. Ensure the repo is clean and running Go 1.23+.
2. Run `go clean -cache` (optional but avoids stale CGO objects).
3. Build locally: `go build ./cmd/schemata`.
4. Run the full test suite: `go test ./...`.

On macOS 15.4+ the build should now succeed without `strchrnul` conflicts.

## Upstream Tracking

We should drop this workaround once pg_query_go ships a version that includes
the upstream PostgreSQL fix. Relevant links:

- https://github.com/pganalyze/libpg_query/issues/276
- https://github.com/pganalyze/libpg_query/issues/282
- https://github.com/pganalyze/pg_query_go/issues/132

When a release becomes available:

1. Remove the `go.mod` replace directive.
2. Delete `third_party/pg_query_go/v5`.
3. Upgrade to the new pg_query_go version and re-run the tests above.

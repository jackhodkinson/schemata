# pg_query_go Patch Contract

This repository intentionally vendors a patched copy of `pg_query_go` under
`third_party/pg_query_go/v5` for macOS compatibility.

## Contract Scope

The local patch is a platform-compatibility shim only. It must not change SQL
parsing semantics.

Allowed patch scope:

- `parser/src_port_snprintf.c`
- `parser/include/postgres/pg_config.h`

Any behavior-altering changes outside this scope require explicit architecture
review and a separate design decision.

## Enforced Contract Test

`test/pgquery_patch_contract_test.go` is the behavioral contract test for the
patched dependency. It validates stable outcomes for:

- `Parse` (statement count fixtures)
- `Normalize` (exact normalized SQL fixtures)
- `Fingerprint` (exact fingerprint fixtures)

This test is included in `make test-pgquery-smoke` and runs in CI on Linux and
macOS. If upstream updates or local patch drift changes behavior, CI must fail
loudly before merge.

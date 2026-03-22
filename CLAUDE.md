# CLAUDE.md Public Contract

This file defines the repository-level AI collaboration contract for `schemata`.

## Purpose

- Keep AI-assisted changes modular, testable, and easy to review.
- Preserve the project architecture: `cmd/`, `internal/`, `pkg/`, and focused tests.
- Prioritize production-safe behavior over speed.

## Required Engineering Standards

- Use `pg_query_go` for SQL parsing. Do not introduce a hand-rolled parser.
- Keep code and tests organized by feature boundary, with small high-signal tests.
- Ensure the project builds and tests cleanly before proposing completion.
- Avoid root-level build artifacts; put generated binaries and compiled tests under `bin/`.

## Canonical Internal Guidance

Detailed engineering guidance lives in `docs/engineering/CLAUDE.md`.

# Archived Documentation

This directory contains historical documentation that has been superseded or resolved.

## Status Documents

Initial implementation status trackers from October 2025 were removed after completion because they were superseded by current code, tests, and CI.

## Root Cause Analysis Documents

- **ROOT_CAUSE_ANALYSIS.md** - pg_query build issue on macOS (✅ RESOLVED)
  - **Resolution:** Fixed with CGO_CFLAGS workaround (see docs/engineering/PG_QUERY_FIX.md)

- **RCA_FK_PARSER_BUG.md** - Foreign key parser bug (✅ RESOLVED)
  - **Resolution:** Fixed by using correct pg_query fields (FkAttrs instead of Keys)

## Current Documentation

For up-to-date documentation, see:

- **README.md** - User documentation and commands
- **ARCHITECTURE.md** - System design and architecture
- **docs/PRODUCTION_READINESS_PLAN.md** - Production readiness roadmap
- **docs/engineering/PG_QUERY_FIX.md** - macOS build instructions
- **docs/NORMALIZATION.md** - Schema normalization system
- **test/integration/README.md** - Integration test documentation
- **issues/** - Active issues and bug reports

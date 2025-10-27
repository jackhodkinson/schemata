# Archived Documentation

This directory contains historical documentation that has been superseded or resolved.

## Status Documents (October 2025)

These documents tracked implementation progress during initial development:

- **IMPLEMENTATION_STATUS.md** - Implementation tracking (2025-10-16)
- **BUILD_STATUS.md** - Build and test status (2025-10-16)
- **PARSER_STATUS.md** - Parser implementation status (2025-10-16)
- **TEST_RESULTS.md** - Test results and false positive fixes (2025-10-17)

**Superseded by:** Current code and test suite

## Root Cause Analysis Documents

- **ROOT_CAUSE_ANALYSIS.md** - pg_query build issue on macOS (✅ RESOLVED)
  - **Resolution:** Fixed with CGO_CFLAGS workaround (see PG_QUERY_FIX.md in root)

- **RCA_FK_PARSER_BUG.md** - Foreign key parser bug (✅ RESOLVED)
  - **Resolution:** Fixed by using correct pg_query fields (FkAttrs instead of Keys)

## Current Documentation

For up-to-date documentation, see:

- **README.md** - User documentation and commands
- **ARCHITECTURE.md** - System design and architecture
- **PLAN.md** - Implementation plan and current status
- **PG_QUERY_FIX.md** - macOS build instructions
- **docs/NORMALIZATION.md** - Schema normalization system
- **test/integration/README.md** - Integration test documentation
- **issues/** - Active issues and bug reports

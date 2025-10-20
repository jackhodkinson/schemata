#!/bin/bash
# End-to-end test that replicates the setup in ../test-schemata
# This test verifies that schemata migrate works correctly

set -e

echo "=== End-to-End Test for Schemata Migrate ==="
echo ""

# Change to test-schemata directory
cd "$(dirname "$0")/../../.."
cd test-schemata

echo "Step 1: Syncing dev database (clean + apply migrations)..."
../schemata/bin/schemata sync
echo "✓ Sync complete"
echo ""

echo "Step 2: Running diff to check for false positives..."
DIFF_OUTPUT=$(../schemata/bin/schemata diff --from migrations 2>&1 || true)

# Check if diff is empty
if echo "$DIFF_OUTPUT" | grep -q "✓ Schemas are in sync"; then
    echo "✅ TEST PASSED: No false positives detected!"
    echo ""
    exit 0
else
    echo "❌ TEST FAILED: False positives detected"
    echo ""
    echo "Diff output:"
    echo "$DIFF_OUTPUT"
    echo ""

    # Parse the diff to identify specific issues
    CREATE_COUNT=$(echo "$DIFF_OUTPUT" | grep -o "Objects to CREATE ([0-9]*)" | grep -o "[0-9]*" || echo "0")
    DROP_COUNT=$(echo "$DIFF_OUTPUT" | grep -o "Objects to DROP ([0-9]*)" | grep -o "[0-9]*" || echo "0")
    ALTER_COUNT=$(echo "$DIFF_OUTPUT" | grep -o "Objects to ALTER ([0-9]*)" | grep -o "[0-9]*" || echo "0")

    echo "Summary:"
    echo "  - $CREATE_COUNT objects to create"
    echo "  - $DROP_COUNT objects to drop"
    echo "  - $ALTER_COUNT objects to alter"
    echo ""

    # Check for specific known issues
    if echo "$DIFF_OUTPUT" | grep -q "function:.*update_updated_at_column"; then
        echo "⚠️  Known Issue: Function body normalization"
        echo "    The same function appears in both CREATE and DROP due to"
        echo "    formatting differences between parser and pg_get_functiondef()"
        echo ""
    fi

    if echo "$DIFF_OUTPUT" | grep -q "key expressions changed"; then
        echo "⚠️  Bug: Index key expression extraction"
        echo "    Index expressions are not being extracted correctly"
        echo ""
    fi

    if echo "$DIFF_OUTPUT" | grep -q "for each row changed"; then
        echo "⚠️  Bug: Trigger ForEachRow parsing"
        echo "    Trigger ForEachRow flag is not being parsed from tgtype bitfield"
        echo ""
    fi

    exit 1
fi

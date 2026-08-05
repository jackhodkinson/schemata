package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestNextvalRegclassExpressionComparison(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		desired schema.Expr
		actual  schema.Expr
		equal   bool
	}{
		"public qualification is catalog-equivalent": {
			desired: `nextval('public.item_ids'::regclass)`,
			actual:  `nextval('item_ids'::regclass)`,
			equal:   true,
		},
		"quoted public qualification is catalog-equivalent": {
			desired: `pg_catalog.nextval('"public"."Item IDs"'::regclass)`,
			actual:  `nextval('"Item IDs"'::regclass)`,
			equal:   true,
		},
		"another schema is not equivalent to unqualified public": {
			desired: `nextval('tenant.item_ids'::regclass)`,
			actual:  `nextval('item_ids'::regclass)`,
			equal:   false,
		},
		"a containing name is not equivalent": {
			desired: `nextval('public.item_ids'::regclass)`,
			actual:  `nextval('not_item_ids'::regclass)`,
			equal:   false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.equal, expressionsEquivalent(test.desired, test.actual))
		})
	}
}

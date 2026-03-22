package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestCompareGrants_IgnoreActualWhenDesiredEmpty(t *testing.T) {
	actual := []schema.Grant{{
		Grantee:    "postgres",
		Privileges: []schema.Privilege{schema.PrivSelect},
	}}
	assert.Empty(t, compareGrants(nil, actual))
	assert.Empty(t, compareGrants([]schema.Grant{}, actual))
}

func TestCompareGrants_AddAndRevoke(t *testing.T) {
	d := []schema.Grant{{
		Grantee:    "alice",
		Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivInsert},
	}}
	a := []schema.Grant{{
		Grantee:    "bob",
		Privileges: []schema.Privilege{schema.PrivDelete},
	}}
	ch := compareGrants(d, a)
	assert.Len(t, ch, 2)
}

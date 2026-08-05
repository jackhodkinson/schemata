package differ

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareGrants_NilIsUnmanagedAndEmptyIsAuthoritative(t *testing.T) {
	actual := []schema.Grant{{
		Grantee:    schema.RoleGrantee("postgres"),
		Privileges: []schema.Privilege{schema.PrivSelect},
	}}
	assert.Empty(t, compareGrants(nil, actual))
	assert.Len(t, compareGrants([]schema.Grant{}, actual), 1)
}

func TestCompareGrants_AddAndRevoke(t *testing.T) {
	d := []schema.Grant{{
		Grantee:    schema.RoleGrantee("alice"),
		Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivInsert},
	}}
	a := []schema.Grant{{
		Grantee:    schema.RoleGrantee("bob"),
		Privileges: []schema.Privilege{schema.PrivDelete},
	}}
	ch := compareGrants(d, a)
	assert.Len(t, ch, 2)
}

func TestCompareGrantsPrivilegeStateTransitions(t *testing.T) {
	t.Parallel()
	app := schema.RoleGrantee("app")

	tests := []struct {
		name    string
		desired []schema.Grant
		actual  []schema.Grant
		want    []schema.Grant
		prefix  string
	}{
		{
			name:    "partial removal keeps remaining privilege",
			desired: []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}}},
			actual:  []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivUpdate}}},
			want:    []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivUpdate}}},
			prefix:  "revoke grant",
		},
		{
			name:    "grantable removal revokes whole privilege",
			desired: []schema.Grant{},
			actual:  []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}, Grantable: true}},
			want:    []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}}},
			prefix:  "revoke grant",
		},
		{
			name:    "downgrade revokes only grant option",
			desired: []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}}},
			actual:  []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}, Grantable: true}},
			want:    []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}, Grantable: true}},
			prefix:  "revoke grant",
		},
		{
			name:    "upgrade grants option",
			desired: []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}, Grantable: true}},
			actual:  []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}}},
			want:    []schema.Grant{{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect}, Grantable: true}},
			prefix:  "add grant",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changes := compareGrants(test.desired, test.actual)
			require.Len(t, changes, 1)
			revoke, grantee, privileges, grantable, ok := ParseGrantChange(changes[0])
			require.True(t, ok)
			assert.Equal(t, test.prefix == "revoke grant", revoke)
			assert.Equal(t, test.want[0].Grantee, grantee)
			assert.ElementsMatch(t, test.want[0].Privileges, privileges)
			assert.Equal(t, test.want[0].Grantable, grantable)
		})
	}
}

func TestCompareGrantsGroupsOnlyEquivalentMixedActions(t *testing.T) {
	t.Parallel()
	app := schema.RoleGrantee("app")
	desired := []schema.Grant{
		{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivInsert, schema.PrivDelete}},
		{Grantee: app, Privileges: []schema.Privilege{schema.PrivUpdate, schema.PrivReferences}, Grantable: true},
	}
	actual := []schema.Grant{
		{Grantee: app, Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivReferences}},
		{Grantee: app, Privileges: []schema.Privilege{schema.PrivUpdate, schema.PrivDelete, schema.PrivTrigger}, Grantable: true},
		{Grantee: schema.PublicGrantee(), Privileges: []schema.Privilege{schema.PrivSelect}},
	}

	type action struct {
		revoke    bool
		grantee   schema.Grantee
		privs     []schema.Privilege
		grantable bool
	}
	var got []action
	for _, change := range compareGrants(desired, actual) {
		revoke, grantee, privileges, grantable, ok := ParseGrantChange(change)
		require.True(t, ok)
		got = append(got, action{revoke: revoke, grantee: grantee, privs: privileges, grantable: grantable})
	}
	require.Len(t, got, 5)
	assert.Contains(t, got, action{grantee: app, privs: []schema.Privilege{schema.PrivInsert}})
	assert.Contains(t, got, action{grantee: app, privs: []schema.Privilege{schema.PrivReferences}, grantable: true})
	assert.Contains(t, got, action{revoke: true, grantee: app, privs: []schema.Privilege{schema.PrivDelete}, grantable: true})
	assert.Contains(t, got, action{revoke: true, grantee: app, privs: []schema.Privilege{schema.PrivTrigger}})
	assert.Contains(t, got, action{revoke: true, grantee: schema.PublicGrantee(), privs: []schema.Privilege{schema.PrivSelect}})
}

func TestCompareCompositesDetectsAttributeOrder(t *testing.T) {
	t.Parallel()

	desired := schema.CompositeDef{Attributes: []schema.CompositeAttr{
		{Name: "left_id", Type: "integer"},
		{Name: "right_id", Type: "integer"},
	}}
	actual := schema.CompositeDef{Attributes: []schema.CompositeAttr{
		{Name: "right_id", Type: "integer"},
		{Name: "left_id", Type: "integer"},
	}}
	assert.Contains(t, compareComposites(desired, actual), "attribute order changed")
}

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanonicalizeGrantsMergesEquivalentEntries(t *testing.T) {
	t.Parallel()

	grants := CanonicalizeGrants([]Grant{
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivSelect}},
		{Grantee: PublicGrantee(), Privileges: []Privilege{PrivSelect}},
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivUpdate, PrivSelect}},
	})

	assert.Equal(t, []Grant{
		{Grantee: PublicGrantee(), Privileges: []Privilege{PrivSelect}},
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivSelect, PrivUpdate}},
	}, grants)
}

func TestCanonicalizeGrantsPreservesNilVersusManagedEmpty(t *testing.T) {
	t.Parallel()
	assert.Nil(t, CanonicalizeGrants(nil))
	assert.NotNil(t, CanonicalizeGrants([]Grant{}))
}

func TestCanonicalizeGrantsUsesMaximumEffectiveGrantState(t *testing.T) {
	t.Parallel()

	grants := CanonicalizeGrants([]Grant{
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivSelect, PrivUpdate}},
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivSelect}, Grantable: true},
	})
	assert.Equal(t, []Grant{
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivUpdate}},
		{Grantee: RoleGrantee("app"), Privileges: []Privilege{PrivSelect}, Grantable: true},
	}, grants)
}

package schema

import (
	"sort"
	"strings"
)

// CanonicalizeGrants reduces ACL entries to PostgreSQL's effective privilege
// state, sorts privileges within each grant, and sorts the grant slice. If the
// same grantee/privilege appears both with and without grant option, the
// grantable state wins: PostgreSQL has one effective state, not two grants.
func CanonicalizeGrants(grants []Grant) []Grant {
	if grants == nil {
		return grants
	}
	type privilegeKey struct {
		grantee   Grantee
		privilege Privilege
	}
	states := make(map[privilegeKey]bool)
	for _, grant := range grants {
		for _, privilege := range grant.Privileges {
			key := privilegeKey{grantee: grant.Grantee, privilege: privilege}
			states[key] = states[key] || grant.Grantable
		}
	}

	type bucketKey struct {
		grantee   Grantee
		grantable bool
	}
	buckets := make(map[bucketKey][]Privilege)
	for key, grantable := range states {
		bucket := bucketKey{grantee: key.grantee, grantable: grantable}
		buckets[bucket] = append(buckets[bucket], key.privilege)
	}

	out := make([]Grant, 0, len(buckets))
	for key, privileges := range buckets {
		if len(privileges) == 0 {
			continue
		}
		sort.Slice(privileges, func(i, j int) bool { return privileges[i] < privileges[j] })
		out = append(out, Grant{Grantee: key.grantee, Privileges: privileges, Grantable: key.grantable})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Grantee.Kind != out[j].Grantee.Kind {
			return out[i].Grantee.Kind < out[j].Grantee.Kind
		}
		if out[i].Grantee.Name != out[j].Grantee.Name {
			return out[i].Grantee.Name < out[j].Grantee.Name
		}
		if out[i].Grantable != out[j].Grantable {
			return !out[i].Grantable && out[j].Grantable
		}
		return strings.Join(privilegeStrings(out[i].Privileges), ",") < strings.Join(privilegeStrings(out[j].Privileges), ",")
	})
	return out
}

func privilegeStrings(p []Privilege) []string {
	s := make([]string, len(p))
	for i := range p {
		s[i] = string(p[i])
	}
	sort.Strings(s)
	return s
}

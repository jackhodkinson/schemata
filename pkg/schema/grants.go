package schema

import (
	"sort"
	"strings"
)

// CanonicalizeGrants sorts privileges within each grant and sorts the grant slice
// for stable hashing and comparison.
func CanonicalizeGrants(grants []Grant) []Grant {
	if len(grants) == 0 {
		return grants
	}
	out := make([]Grant, len(grants))
	copy(out, grants)
	for i := range out {
		p := make([]Privilege, len(out[i].Privileges))
		copy(p, out[i].Privileges)
		sort.Slice(p, func(a, b int) bool { return p[a] < p[b] })
		out[i].Privileges = p
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Grantee != out[j].Grantee {
			return out[i].Grantee < out[j].Grantee
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

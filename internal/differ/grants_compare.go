package differ

import (
	"sort"
	"strconv"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

func grantKey(g schema.Grant) string {
	ps := make([]string, len(g.Privileges))
	for i := range g.Privileges {
		ps[i] = string(g.Privileges[i])
	}
	sort.Strings(ps)
	gb := "0"
	if g.Grantable {
		gb = "1"
	}
	return g.Grantee + "\x00" + strings.Join(ps, ",") + "\x00" + gb
}

// compareGrants returns add/revoke grant change strings (tab-separated payloads).
// If desired has no grants, actual grants are ignored (same rule as Owner: unspecified desired means "no opinion").
func compareGrants(desired, actual []schema.Grant) []string {
	if len(desired) == 0 {
		return nil
	}
	d := schema.CanonicalizeGrants(desired)
	a := schema.CanonicalizeGrants(actual)

	dm := make(map[string]schema.Grant, len(d))
	for _, g := range d {
		dm[grantKey(g)] = g
	}
	am := make(map[string]schema.Grant, len(a))
	for _, g := range a {
		am[grantKey(g)] = g
	}

	var changes []string
	for k, g := range dm {
		if _, ok := am[k]; !ok {
			changes = append(changes, formatGrantChange("add grant", g))
		}
	}
	for k, g := range am {
		if _, ok := dm[k]; !ok {
			changes = append(changes, formatGrantChange("revoke grant", g))
		}
	}
	sort.Strings(changes)
	return changes
}

func formatGrantChange(prefix string, g schema.Grant) string {
	ps := make([]string, len(g.Privileges))
	for i := range g.Privileges {
		ps[i] = string(g.Privileges[i])
	}
	sort.Strings(ps)
	return prefix + "\t" + g.Grantee + "\t" + strings.Join(ps, ",") + "\t" + strconv.FormatBool(g.Grantable)
}

// ParseGrantChange parses strings from formatGrantChange.
func ParseGrantChange(s string) (revoke bool, grantee string, privs []schema.Privilege, grantable bool, ok bool) {
	parts := strings.Split(s, "\t")
	if len(parts) != 4 {
		return false, "", nil, false, false
	}
	switch parts[0] {
	case "add grant":
		revoke = false
	case "revoke grant":
		revoke = true
	default:
		return false, "", nil, false, false
	}
	grantee = parts[1]
	if parts[2] != "" {
		for _, p := range strings.Split(parts[2], ",") {
			if p != "" {
				privs = append(privs, schema.Privilege(p))
			}
		}
	}
	var err error
	grantable, err = strconv.ParseBool(parts[3])
	if err != nil {
		return false, "", nil, false, false
	}
	return revoke, grantee, privs, grantable, true
}

package differ

import (
	"encoding/base64"
	"sort"
	"strconv"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// compareGrants returns add/revoke grant change strings (tab-separated payloads).
// A nil desired slice means "no opinion". A non-nil empty slice is an
// authoritative request for no ACL entries and therefore revokes actual
// entries. This distinction lets declarative REVOKE statements represent the
// removal of PostgreSQL default grants.
func compareGrants(desired, actual []schema.Grant) []string {
	if desired == nil {
		return nil
	}
	d := schema.CanonicalizeGrants(desired)
	a := schema.CanonicalizeGrants(actual)

	type privilegeKey struct {
		grantee   schema.Grantee
		privilege schema.Privilege
	}
	// State is 0=absent, 1=granted, 2=granted with grant option.
	states := func(grants []schema.Grant) map[privilegeKey]uint8 {
		out := make(map[privilegeKey]uint8)
		for _, grant := range grants {
			state := uint8(1)
			if grant.Grantable {
				state = 2
			}
			for _, privilege := range grant.Privileges {
				key := privilegeKey{grantee: grant.Grantee, privilege: privilege}
				if state > out[key] {
					out[key] = state
				}
			}
		}
		return out
	}
	desiredStates := states(d)
	actualStates := states(a)

	type actionKey struct {
		prefix    string
		grantee   schema.Grantee
		grantable bool
	}
	actions := make(map[actionKey][]schema.Privilege)
	allKeys := make(map[privilegeKey]struct{}, len(desiredStates)+len(actualStates))
	for key := range desiredStates {
		allKeys[key] = struct{}{}
	}
	for key := range actualStates {
		allKeys[key] = struct{}{}
	}
	for key := range allKeys {
		desiredState := desiredStates[key]
		actualState := actualStates[key]
		if desiredState == actualState {
			continue
		}

		var action actionKey
		switch {
		case desiredState == 0:
			// Removing a privilege is always a whole-privilege REVOKE, even
			// when the actual privilege currently carries grant option.
			action = actionKey{prefix: "revoke grant", grantee: key.grantee}
		case actualState == 2 && desiredState == 1:
			// Downgrade while retaining the base privilege.
			action = actionKey{prefix: "revoke grant", grantee: key.grantee, grantable: true}
		case desiredState == 2:
			// Both absent->grantable and plain->grantable use GRANT ... WITH
			// GRANT OPTION.
			action = actionKey{prefix: "add grant", grantee: key.grantee, grantable: true}
		default:
			// The remaining transition is absent->plain.
			action = actionKey{prefix: "add grant", grantee: key.grantee}
		}
		actions[action] = append(actions[action], key.privilege)
	}

	changes := make([]string, 0, len(actions))
	for action, privileges := range actions {
		changes = append(changes, formatGrantChange(action.prefix, schema.Grant{
			Grantee: action.grantee, Privileges: privileges, Grantable: action.grantable,
		}))
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
	name := base64.RawURLEncoding.EncodeToString([]byte(g.Grantee.Name))
	return prefix + "\t" + string(g.Grantee.Kind) + "\t" + name + "\t" + strings.Join(ps, ",") + "\t" + strconv.FormatBool(g.Grantable)
}

// ParseGrantChange parses strings from formatGrantChange.
func ParseGrantChange(s string) (revoke bool, grantee schema.Grantee, privs []schema.Privilege, grantable bool, ok bool) {
	parts := strings.Split(s, "\t")
	if len(parts) != 5 {
		return false, schema.Grantee{}, nil, false, false
	}
	switch parts[0] {
	case "add grant":
		revoke = false
	case "revoke grant":
		revoke = true
	default:
		return false, schema.Grantee{}, nil, false, false
	}
	grantee.Kind = schema.GranteeKind(parts[1])
	decodedName, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false, schema.Grantee{}, nil, false, false
	}
	grantee.Name = string(decodedName)
	if (grantee.Kind == schema.GranteePublic && grantee.Name != "") || (grantee.Kind == schema.GranteeRole && grantee.Name == "") || (grantee.Kind != schema.GranteePublic && grantee.Kind != schema.GranteeRole) {
		return false, schema.Grantee{}, nil, false, false
	}
	if parts[3] != "" {
		for _, p := range strings.Split(parts[3], ",") {
			if p != "" {
				privs = append(privs, schema.Privilege(p))
			}
		}
	}
	grantable, err = strconv.ParseBool(parts[4])
	if err != nil {
		return false, schema.Grantee{}, nil, false, false
	}
	return revoke, grantee, privs, grantable, true
}

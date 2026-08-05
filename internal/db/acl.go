package db

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// extractRelationACL loads ACL grants for a pg_class row (table, view, sequence, etc.).
func (c *Catalog) extractRelationACL(ctx context.Context, oid uint32) ([]schema.Grant, error) {
	q := `
		SELECT
			priv.grantee = 0 AS is_public,
			COALESCE(grantee.rolname, '') AS grantee,
			priv.privilege_type,
			priv.is_grantable,
			priv.grantor = c.relowner AS owner_is_grantor
		FROM pg_class c
		CROSS JOIN LATERAL aclexplode(COALESCE(
			c.relacl,
			acldefault(
				CASE
					WHEN c.relkind IN ('v', 'm') THEN 'r'::"char"
					WHEN c.relkind = 'S' THEN 's'::"char"
					ELSE c.relkind
				END,
				c.relowner
			)
		)) AS priv
		LEFT JOIN pg_roles grantee ON grantee.oid = priv.grantee
		WHERE c.oid = $1
	`
	rows, err := c.pool.Query(ctx, q, oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		isPublic     bool
		grantee      string
		priv         string
		grantable    bool
		ownerGrantor bool
	}
	var raw []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.isPublic, &r.grantee, &r.priv, &r.grantable, &r.ownerGrantor); err != nil {
			return nil, err
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	type gk struct {
		grantee   schema.Grantee
		grantable bool
	}
	buckets := make(map[gk][]schema.Privilege)
	for _, rw := range raw {
		if !rw.ownerGrantor {
			return nil, fmt.Errorf("relation ACL for object %d has a grantor other than the object owner; GRANTED BY is not modeled", oid)
		}
		if !rw.isPublic && rw.grantee == "" {
			return nil, fmt.Errorf("relation ACL for object %d refers to an unresolved grantee role", oid)
		}
		p, err := privilegeFromACL(rw.priv)
		if err != nil {
			return nil, fmt.Errorf("relation ACL for object %d: %w", oid, err)
		}
		grantee := schema.RoleGrantee(rw.grantee)
		if rw.isPublic {
			grantee = schema.PublicGrantee()
		}
		k := gk{grantee: grantee, grantable: rw.grantable}
		buckets[k] = append(buckets[k], p)
	}

	// Preserve the distinction between an explicitly empty ACL (managed and
	// authoritative) and an unmanaged ACL. aclexplode('{}') returns no rows,
	// so a nil result here would silently turn an explicit REVOKE ALL into
	// unmanaged metadata during dump/replay.
	out := make([]schema.Grant, 0, len(buckets))
	for k, privs := range buckets {
		sort.Slice(privs, func(i, j int) bool { return privs[i] < privs[j] })
		out = append(out, schema.Grant{
			Grantee:    k.grantee,
			Privileges: privs,
			Grantable:  k.grantable,
		})
	}
	sort.Slice(out, func(i, j int) bool {
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
	return out, nil
}

func privilegeStrings(p []schema.Privilege) []string {
	s := make([]string, len(p))
	for i := range p {
		s[i] = string(p[i])
	}
	sort.Strings(s)
	return s
}

func privilegeFromACL(s string) (schema.Privilege, error) {
	u := strings.ToUpper(strings.TrimSpace(s))
	switch u {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN":
		return schema.Privilege(u), nil
	case "USAGE":
		return schema.PrivUsage, nil
	case "CREATE":
		return schema.PrivCreate, nil
	case "EXECUTE":
		return schema.PrivExecute, nil
	default:
		return "", fmt.Errorf("unsupported PostgreSQL privilege %q; refusing to omit it from the declarative model", s)
	}
}

// extractFunctionACL loads EXECUTE grants for a pg_proc row.
func (c *Catalog) extractFunctionACL(ctx context.Context, oid uint32) ([]schema.Grant, error) {
	q := `
		SELECT
			priv.grantee = 0 AS is_public,
			COALESCE(grantee.rolname, '') AS grantee,
			priv.privilege_type,
			priv.is_grantable,
			priv.grantor = p.proowner AS owner_is_grantor
		FROM pg_proc p
		CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f'::"char", p.proowner))) AS priv
		LEFT JOIN pg_roles grantee ON grantee.oid = priv.grantee
		WHERE p.oid = $1
	`
	rows, err := c.pool.Query(ctx, q, oid)
	if err != nil {
		return nil, fmt.Errorf("function acl: %w", err)
	}
	defer rows.Close()

	type row struct {
		isPublic     bool
		grantee      string
		priv         string
		grantable    bool
		ownerGrantor bool
	}
	var raw []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.isPublic, &r.grantee, &r.priv, &r.grantable, &r.ownerGrantor); err != nil {
			return nil, err
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	type gk struct {
		grantee   schema.Grantee
		grantable bool
	}
	buckets := make(map[gk][]schema.Privilege)
	for _, rw := range raw {
		if !rw.ownerGrantor {
			return nil, fmt.Errorf("function ACL for object %d has a grantor other than the object owner; GRANTED BY is not modeled", oid)
		}
		if !rw.isPublic && rw.grantee == "" {
			return nil, fmt.Errorf("function ACL for object %d refers to an unresolved grantee role", oid)
		}
		p, err := privilegeFromACL(rw.priv)
		if err != nil {
			return nil, fmt.Errorf("function ACL for object %d: %w", oid, err)
		}
		grantee := schema.RoleGrantee(rw.grantee)
		if rw.isPublic {
			grantee = schema.PublicGrantee()
		}
		k := gk{grantee: grantee, grantable: rw.grantable}
		buckets[k] = append(buckets[k], p)
	}

	// See extractRelationACL: an explicit empty proacl must remain a non-nil
	// empty slice so dump/replay does not restore PostgreSQL's default ACL.
	out := make([]schema.Grant, 0, len(buckets))
	for k, privs := range buckets {
		sort.Slice(privs, func(i, j int) bool { return privs[i] < privs[j] })
		out = append(out, schema.Grant{
			Grantee:    k.grantee,
			Privileges: privs,
			Grantable:  k.grantable,
		})
	}
	sort.Slice(out, func(i, j int) bool {
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
	return out, nil
}

// extractTypeACL loads USAGE grants for an enum/domain/composite pg_type row.
func (c *Catalog) extractTypeACL(ctx context.Context, oid uint32) ([]schema.Grant, error) {
	q := `
		SELECT
			priv.grantee = 0 AS is_public,
			COALESCE(grantee.rolname, '') AS grantee,
			priv.privilege_type,
			priv.is_grantable,
			priv.grantor = t.typowner AS owner_is_grantor
		FROM pg_type t
		CROSS JOIN LATERAL aclexplode(COALESCE(t.typacl, acldefault('T'::"char", t.typowner))) AS priv
		LEFT JOIN pg_roles grantee ON grantee.oid = priv.grantee
		WHERE t.oid = $1
	`
	rows, err := c.pool.Query(ctx, q, oid)
	if err != nil {
		return nil, fmt.Errorf("type acl: %w", err)
	}
	defer rows.Close()

	type grantKey struct {
		grantee   schema.Grantee
		grantable bool
	}
	buckets := make(map[grantKey][]schema.Privilege)
	for rows.Next() {
		var isPublic bool
		var roleName, privilege string
		var grantable, ownerGrantor bool
		if err := rows.Scan(&isPublic, &roleName, &privilege, &grantable, &ownerGrantor); err != nil {
			return nil, err
		}
		if !ownerGrantor {
			return nil, fmt.Errorf("type ACL for object %d has a grantor other than the object owner; GRANTED BY is not modeled", oid)
		}
		if !isPublic && roleName == "" {
			return nil, fmt.Errorf("type ACL for object %d refers to an unresolved grantee role", oid)
		}
		parsed, err := privilegeFromACL(privilege)
		if err != nil {
			return nil, fmt.Errorf("type ACL for object %d: %w", oid, err)
		}
		grantee := schema.RoleGrantee(roleName)
		if isPublic {
			grantee = schema.PublicGrantee()
		}
		key := grantKey{grantee: grantee, grantable: grantable}
		buckets[key] = append(buckets[key], parsed)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]schema.Grant, 0, len(buckets))
	for key, privileges := range buckets {
		out = append(out, schema.Grant{Grantee: key.grantee, Privileges: privileges, Grantable: key.grantable})
	}
	return schema.CanonicalizeGrants(out), nil
}

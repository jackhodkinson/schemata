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
			COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
			priv.privilege_type,
			priv.is_grantable
		FROM pg_class c
		CROSS JOIN LATERAL aclexplode(COALESCE(
			c.relacl,
			acldefault(
				CASE WHEN c.relkind IN ('v', 'm') THEN 'r'::"char" ELSE c.relkind END,
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
		grantee   string
		priv      string
		grantable bool
	}
	var raw []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.grantee, &r.priv, &r.grantable); err != nil {
			return nil, err
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	type gk struct {
		grantee   string
		grantable bool
	}
	buckets := make(map[gk][]schema.Privilege)
	for _, rw := range raw {
		p := privilegeFromACL(rw.priv)
		if p == "" {
			continue
		}
		k := gk{grantee: rw.grantee, grantable: rw.grantable}
		buckets[k] = append(buckets[k], p)
	}

	var out []schema.Grant
	for k, privs := range buckets {
		sort.Slice(privs, func(i, j int) bool { return privs[i] < privs[j] })
		out = append(out, schema.Grant{
			Grantee:    k.grantee,
			Privileges: privs,
			Grantable:  k.grantable,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Grantee != out[j].Grantee {
			return out[i].Grantee < out[j].Grantee
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

func privilegeFromACL(s string) schema.Privilege {
	u := strings.ToUpper(strings.TrimSpace(s))
	switch u {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER":
		return schema.Privilege(u)
	case "USAGE":
		return schema.PrivUsage
	case "CREATE":
		return schema.PrivCreate
	case "EXECUTE":
		return schema.PrivExecute
	default:
		if u == "" {
			return ""
		}
		return schema.Privilege(u)
	}
}

// extractFunctionACL loads EXECUTE grants for a pg_proc row.
func (c *Catalog) extractFunctionACL(ctx context.Context, oid uint32) ([]schema.Grant, error) {
	q := `
		SELECT
			COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
			priv.privilege_type,
			priv.is_grantable
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
		grantee   string
		priv      string
		grantable bool
	}
	var raw []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.grantee, &r.priv, &r.grantable); err != nil {
			return nil, err
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	type gk struct {
		grantee   string
		grantable bool
	}
	buckets := make(map[gk][]schema.Privilege)
	for _, rw := range raw {
		p := privilegeFromACL(rw.priv)
		if p == "" {
			continue
		}
		k := gk{grantee: rw.grantee, grantable: rw.grantable}
		buckets[k] = append(buckets[k], p)
	}

	var out []schema.Grant
	for k, privs := range buckets {
		sort.Slice(privs, func(i, j int) bool { return privs[i] < privs[j] })
		out = append(out, schema.Grant{
			Grantee:    k.grantee,
			Privileges: privs,
			Grantable:  k.grantable,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Grantee != out[j].Grantee {
			return out[i].Grantee < out[j].Grantee
		}
		if out[i].Grantable != out[j].Grantable {
			return !out[i].Grantable && out[j].Grantable
		}
		return strings.Join(privilegeStrings(out[i].Privileges), ",") < strings.Join(privilegeStrings(out[j].Privileges), ",")
	})
	return out, nil
}

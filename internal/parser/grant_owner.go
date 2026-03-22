package parser

import (
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func (p *Parser) mergeGrantsAndOwners(objects []schema.DatabaseObject, result *pg_query.ParseResult) []schema.DatabaseObject {
	for _, rawStmt := range result.Stmts {
		if rawStmt.Stmt == nil {
			continue
		}
		switch n := rawStmt.Stmt.Node.(type) {
		case *pg_query.Node_GrantStmt:
			if n.GrantStmt != nil {
				objects = mergeGrantStmt(objects, n.GrantStmt)
			}
		case *pg_query.Node_AlterOwnerStmt:
			if n.AlterOwnerStmt != nil {
				objects = mergeAlterOwnerStmt(objects, n.AlterOwnerStmt)
			}
		}
	}
	return objects
}

func mergeGrantStmt(objects []schema.DatabaseObject, stmt *pg_query.GrantStmt) []schema.DatabaseObject {
	if stmt == nil || !stmt.IsGrant {
		return objects
	}

	privs := privilegeNodesToList(stmt.Privileges)
	if len(privs) == 0 {
		switch stmt.Objtype {
		case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_MATVIEW:
			privs = []schema.Privilege{
				schema.PrivSelect, schema.PrivInsert, schema.PrivUpdate, schema.PrivDelete,
				schema.PrivTruncate, schema.PrivReferences, schema.PrivTrigger,
			}
		case pg_query.ObjectType_OBJECT_SEQUENCE:
			privs = []schema.Privilege{schema.PrivUsage, schema.PrivSelect, schema.PrivUpdate}
		case pg_query.ObjectType_OBJECT_FUNCTION:
			privs = []schema.Privilege{schema.PrivExecute}
		default:
			privs = []schema.Privilege{schema.PrivAll}
		}
	}

	for _, objNode := range stmt.Objects {
		schemaName, objName, ok := objectNameFromGrantNode(objNode)
		if !ok {
			continue
		}
		for _, granteeNode := range stmt.Grantees {
			ge := granteeName(granteeNode)
			if ge == "" {
				continue
			}
			g := schema.Grant{
				Grantee:    ge,
				Privileges: append([]schema.Privilege(nil), privs...),
				Grantable:  stmt.GrantOption,
			}
			objects = attachGrant(objects, stmt.Objtype, schemaName, objName, g)
		}
	}
	return objects
}

func attachGrant(objects []schema.DatabaseObject, objType pg_query.ObjectType, sn schema.SchemaName, name string, g schema.Grant) []schema.DatabaseObject {
	switch objType {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_MATVIEW:
		for i := range objects {
			if tbl, ok := objects[i].(schema.Table); ok && tbl.Schema == sn && string(tbl.Name) == name {
				tbl.Grants = append(tbl.Grants, g)
				objects[i] = tbl
				return objects
			}
		}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		for i := range objects {
			if seq, ok := objects[i].(schema.Sequence); ok && seq.Schema == sn && seq.Name == name {
				seq.Grants = append(seq.Grants, g)
				objects[i] = seq
				return objects
			}
		}
	case pg_query.ObjectType_OBJECT_FUNCTION:
		for i := range objects {
			if fn, ok := objects[i].(schema.Function); ok && fn.Schema == sn && fn.Name == name {
				fn.Grants = append(fn.Grants, g)
				objects[i] = fn
				return objects
			}
		}
	}
	return objects
}

func objectNameFromGrantNode(node *pg_query.Node) (schema.SchemaName, string, bool) {
	if node == nil {
		return "", "", false
	}
	if rv := node.GetRangeVar(); rv != nil {
		sn, rel := extractRangeVarName(rv)
		return sn, rel, true
	}
	return "", "", false
}

func extractRangeVarName(rv *pg_query.RangeVar) (schema.SchemaName, string) {
	if rv == nil {
		return schema.SchemaName("public"), ""
	}
	sn := schema.SchemaName("public")
	if rv.Schemaname != "" {
		sn = schema.SchemaName(rv.Schemaname)
	}
	return sn, rv.Relname
}

func granteeName(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if rs := node.GetRoleSpec(); rs != nil {
		return rs.GetRolename()
	}
	return ""
}

func privilegeNodesToList(nodes []*pg_query.Node) []schema.Privilege {
	var out []schema.Privilege
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if ap := n.GetAccessPriv(); ap != nil {
			if ap.PrivName == "" {
				continue
			}
			out = append(out, schema.Privilege(strings.ToUpper(strings.TrimSpace(ap.PrivName))))
		}
	}
	return out
}

func listNodeStrings(nodes []*pg_query.Node) []string {
	var out []string
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if sn := n.GetString_(); sn != nil {
			out = append(out, sn.Sval)
		}
	}
	return out
}

func mergeAlterOwnerStmt(objects []schema.DatabaseObject, stmt *pg_query.AlterOwnerStmt) []schema.DatabaseObject {
	if stmt == nil || stmt.Newowner == nil {
		return objects
	}
	newOwner := stmt.Newowner.GetRolename()
	if newOwner == "" {
		return objects
	}

	switch stmt.ObjectType {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_MATVIEW:
		if stmt.Relation == nil {
			return objects
		}
		sn, rel := extractRangeVarName(stmt.Relation)
		for i := range objects {
			if tbl, ok := objects[i].(schema.Table); ok && tbl.Schema == sn && string(tbl.Name) == rel {
				t := tbl
				t.Owner = strPtr(newOwner)
				objects[i] = t
				return objects
			}
		}
	case pg_query.ObjectType_OBJECT_VIEW:
		if stmt.Relation == nil {
			return objects
		}
		sn, rel := extractRangeVarName(stmt.Relation)
		for i := range objects {
			if v, ok := objects[i].(schema.View); ok && v.Schema == sn && v.Name == rel {
				v.Owner = strPtr(newOwner)
				objects[i] = v
				return objects
			}
		}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		if stmt.Relation == nil {
			return objects
		}
		sn, rel := extractRangeVarName(stmt.Relation)
		for i := range objects {
			if seq, ok := objects[i].(schema.Sequence); ok && seq.Schema == sn && seq.Name == rel {
				seq.Owner = strPtr(newOwner)
				objects[i] = seq
				return objects
			}
		}
	case pg_query.ObjectType_OBJECT_FUNCTION:
		if stmt.Object == nil {
			return objects
		}
		ow := stmt.Object.GetObjectWithArgs()
		if ow == nil {
			return objects
		}
		names := listNodeStrings(ow.Objname)
		if len(names) == 0 {
			return objects
		}
		sn := schema.SchemaName("public")
		fnName := names[len(names)-1]
		if len(names) >= 2 {
			sn = schema.SchemaName(names[len(names)-2])
		}
		for i := range objects {
			if fn, ok := objects[i].(schema.Function); ok && fn.Schema == sn && fn.Name == fnName {
				fn.Owner = strPtr(newOwner)
				objects[i] = fn
				return objects
			}
		}
	}
	return objects
}

func strPtr(s string) *string { return &s }

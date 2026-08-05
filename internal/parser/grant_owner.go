package parser

import (
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/internal/objectmap"
	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// mergeGrantsAndOwners applies standalone ACL/owner statements to the object
// they describe. Metadata is not allowed to disappear: every statement must
// resolve to exactly one object extracted from the same declarative input.
func (p *Parser) mergeGrantsAndOwners(objects []schema.DatabaseObject, result *pg_query.ParseResult) ([]schema.DatabaseObject, error) {
	for _, rawStmt := range result.Stmts {
		if rawStmt.Stmt == nil {
			continue
		}
		var err error
		switch n := rawStmt.Stmt.Node.(type) {
		case *pg_query.Node_GrantStmt:
			objects, err = p.mergeGrantStmt(objects, n.GrantStmt)
		case *pg_query.Node_AlterOwnerStmt:
			objects, err = p.mergeAlterOwnerStmt(objects, n.AlterOwnerStmt)
		case *pg_query.Node_AlterTableStmt:
			if isOwnerOnlyAlterTableStmt(n.AlterTableStmt) {
				objects, err = p.mergeAlterTableOwnerStmt(objects, n.AlterTableStmt)
			}
		case *pg_query.Node_AlterSeqStmt:
			if isOwnedByOnlyAlterSequenceStmt(n.AlterSeqStmt) {
				objects, err = p.mergeAlterSequenceOwnedByStmt(objects, n.AlterSeqStmt)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return objects, nil
}

func (p *Parser) mergeGrantStmt(objects []schema.DatabaseObject, stmt *pg_query.GrantStmt) ([]schema.DatabaseObject, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil GRANT statement")
	}
	switch stmt.Behavior {
	case pg_query.DropBehavior_DROP_BEHAVIOR_UNDEFINED, pg_query.DropBehavior_DROP_RESTRICT:
		// RESTRICT is PostgreSQL's default and is the behavior emitted by the
		// planner, so spelling it explicitly does not change the modeled state.
	case pg_query.DropBehavior_DROP_CASCADE:
		return nil, fmt.Errorf("REVOKE CASCADE is not modeled because it can remove dependent grants; use RESTRICT or an explicit migration")
	default:
		return nil, fmt.Errorf("unsupported GRANT/REVOKE dependency behavior %s", stmt.Behavior)
	}
	if stmt.Grantor != nil {
		return nil, fmt.Errorf("GRANTED BY is not modeled; omit the grantor or use an explicit migration")
	}
	if stmt.Targtype != pg_query.GrantTargetType_ACL_TARGET_OBJECT {
		return nil, fmt.Errorf("unsupported GRANT target mode %s; name each modeled object explicitly", stmt.Targtype)
	}
	if len(stmt.Objects) == 0 || len(stmt.Grantees) == 0 {
		return nil, fmt.Errorf("GRANT must name at least one object and grantee")
	}

	privs, err := privilegesForGrant(stmt)
	if err != nil {
		return nil, err
	}
	grantees := make([]schema.Grantee, 0, len(stmt.Grantees))
	for _, node := range stmt.Grantees {
		grantee, err := granteeFromNode(node)
		if err != nil {
			return nil, err
		}
		grantees = append(grantees, grantee)
	}

	for _, objNode := range stmt.Objects {
		target, err := p.grantTarget(stmt.Objtype, objNode)
		if err != nil {
			return nil, err
		}
		for _, grantee := range grantees {
			grant := schema.Grant{
				Grantee:    grantee,
				Privileges: append([]schema.Privilege(nil), privs...),
				Grantable:  stmt.GrantOption,
			}
			if stmt.IsGrant {
				objects, err = attachGrantExactlyOnce(objects, target, grant)
			} else {
				objects, err = revokeGrantExactlyOnce(objects, target, grant)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return objects, nil
}

type metadataTarget struct {
	kind      schema.ObjectKind
	schema    schema.SchemaName
	name      string
	signature string
	viewType  *schema.ViewType
	// PostgreSQL spells grants on views as GRANT ... ON TABLE. This flag is
	// intentionally restricted to GRANT targets so COMMENT/OWNER cannot cross
	// object families.
	tableGrantSurface bool
}

func (t metadataTarget) String() string {
	identity := fmt.Sprintf("%s %s.%s", t.kind, t.schema, t.name)
	if t.signature != "" {
		identity += t.signature
	}
	return identity
}

func (p *Parser) grantTarget(objType pg_query.ObjectType, node *pg_query.Node) (metadataTarget, error) {
	switch objType {
	case pg_query.ObjectType_OBJECT_TABLE:
		sn, name, err := relationGrantName(node)
		return metadataTarget{kind: schema.TableKind, schema: sn, name: name, tableGrantSurface: true}, err
	case pg_query.ObjectType_OBJECT_VIEW:
		sn, name, err := relationGrantName(node)
		viewType := schema.RegularView
		return metadataTarget{kind: schema.ViewKind, schema: sn, name: name, viewType: &viewType}, err
	case pg_query.ObjectType_OBJECT_MATVIEW:
		sn, name, err := relationGrantName(node)
		viewType := schema.MaterializedView
		return metadataTarget{kind: schema.ViewKind, schema: sn, name: name, viewType: &viewType}, err
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		sn, name, err := relationGrantName(node)
		return metadataTarget{kind: schema.SequenceKind, schema: sn, name: name}, err
	case pg_query.ObjectType_OBJECT_FUNCTION:
		sn, name, signature, err := p.objectWithArgsIdentity(node)
		return metadataTarget{kind: schema.FunctionKind, schema: sn, name: name, signature: signature}, err
	case pg_query.ObjectType_OBJECT_TYPE, pg_query.ObjectType_OBJECT_DOMAIN:
		sn, name, err := qualifiedNameNode(node)
		return metadataTarget{kind: schema.TypeKind, schema: sn, name: name}, err
	default:
		return metadataTarget{}, fmt.Errorf("unsupported GRANT object type %s; only modeled tables, views, materialized views, sequences, functions, and types are supported", objType)
	}
}

func relationGrantName(node *pg_query.Node) (schema.SchemaName, string, error) {
	if node == nil || node.GetRangeVar() == nil {
		return "", "", fmt.Errorf("GRANT relation target has an unsupported AST shape")
	}
	sn, name := extractRangeVarName(node.GetRangeVar())
	if name == "" {
		return "", "", fmt.Errorf("GRANT relation target is missing an object name")
	}
	return sn, name, nil
}

func qualifiedNameNode(node *pg_query.Node) (schema.SchemaName, string, error) {
	if node == nil {
		return "", "", fmt.Errorf("metadata target is missing an object name")
	}
	var names []string
	if typeName := node.GetTypeName(); typeName != nil {
		names = listNodeStrings(typeName.Names)
	} else {
		names = extractStringList(node)
	}
	if len(names) < 1 || len(names) > 2 {
		return "", "", fmt.Errorf("metadata target must have one or two name components, got %d", len(names))
	}
	sn := schema.SchemaName("public")
	if len(names) == 2 {
		sn = schema.SchemaName(names[0])
	}
	return sn, names[len(names)-1], nil
}

func (p *Parser) objectWithArgsIdentity(node *pg_query.Node) (schema.SchemaName, string, string, error) {
	if node == nil || node.GetObjectWithArgs() == nil {
		return "", "", "", fmt.Errorf("function metadata target must include an exact function signature")
	}
	ow := node.GetObjectWithArgs()
	names := listNodeStrings(ow.Objname)
	if len(names) < 1 || len(names) > 2 {
		return "", "", "", fmt.Errorf("function metadata target must have one or two name components, got %d", len(names))
	}
	if ow.ArgsUnspecified {
		return "", "", "", fmt.Errorf("function metadata target %s does not specify argument types; overloaded functions require an exact identity signature", strings.Join(names, "."))
	}

	args := make([]schema.FunctionArg, 0, len(ow.Objargs))
	for _, argNode := range ow.Objargs {
		if argNode == nil || argNode.GetTypeName() == nil {
			return "", "", "", fmt.Errorf("function metadata target %s has an unsupported argument type", strings.Join(names, "."))
		}
		typeName, err := p.parseTypeName(argNode.GetTypeName())
		if err != nil {
			return "", "", "", fmt.Errorf("function metadata target %s has an invalid argument type: %w", strings.Join(names, "."), err)
		}
		args = append(args, schema.FunctionArg{Mode: schema.InMode, Type: typeName})
	}

	sn := schema.SchemaName("public")
	if len(names) == 2 {
		sn = schema.SchemaName(names[0])
	}
	return sn, names[len(names)-1], schema.FunctionSignature(args), nil
}

func attachGrantExactlyOnce(objects []schema.DatabaseObject, target metadataTarget, grant schema.Grant) ([]schema.DatabaseObject, error) {
	return mutateGrantsExactlyOnce(objects, target, "GRANT", func(grants []schema.Grant) []schema.Grant {
		return append(grants, grant)
	})
}

func revokeGrantExactlyOnce(objects []schema.DatabaseObject, target metadataTarget, grant schema.Grant) ([]schema.DatabaseObject, error) {
	return mutateGrantsExactlyOnce(objects, target, "REVOKE", func(grants []schema.Grant) []schema.Grant {
		if grants == nil {
			grants = []schema.Grant{}
		}
		canonical := schema.CanonicalizeGrants(grants)
		remove := make(map[schema.Privilege]struct{}, len(grant.Privileges))
		for _, privilege := range grant.Privileges {
			remove[privilege] = struct{}{}
		}
		var downgraded []schema.Privilege
		out := make([]schema.Grant, 0, len(canonical)+1)
		for _, existing := range canonical {
			if existing.Grantee != grant.Grantee || (grant.Grantable && !existing.Grantable) {
				out = append(out, existing)
				continue
			}
			kept := make([]schema.Privilege, 0, len(existing.Privileges))
			for _, privilege := range existing.Privileges {
				if _, revoked := remove[privilege]; revoked {
					if grant.Grantable && existing.Grantable {
						downgraded = append(downgraded, privilege)
					}
					continue
				}
				kept = append(kept, privilege)
			}
			if len(kept) > 0 {
				existing.Privileges = kept
				out = append(out, existing)
			}
		}
		if len(downgraded) > 0 {
			out = append(out, schema.Grant{Grantee: grant.Grantee, Privileges: downgraded})
		}
		return schema.CanonicalizeGrants(out)
	})
}

func mutateGrantsExactlyOnce(objects []schema.DatabaseObject, target metadataTarget, statement string, mutate func([]schema.Grant) []schema.Grant) ([]schema.DatabaseObject, error) {
	matches := 0
	for i := range objects {
		switch obj := objects[i].(type) {
		case schema.Table:
			// GRANT ... ON TABLE is PostgreSQL's syntax for tables and both
			// view kinds. A table match is therefore one possible resolution.
			if target.kind == schema.TableKind && obj.Schema == target.schema && string(obj.Name) == target.name {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		case schema.View:
			if (target.tableGrantSurface || target.kind == schema.ViewKind) && obj.Schema == target.schema && obj.Name == target.name && (target.viewType == nil || obj.Type == *target.viewType) {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		case schema.Sequence:
			if target.kind == schema.SequenceKind && obj.Schema == target.schema && obj.Name == target.name {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		case schema.Function:
			if target.kind == schema.FunctionKind && obj.Schema == target.schema && obj.Name == target.name && schema.FunctionSignature(obj.Args) == target.signature {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		case schema.EnumDef:
			if target.kind == schema.TypeKind && obj.Schema == target.schema && string(obj.Name) == target.name {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		case schema.DomainDef:
			if target.kind == schema.TypeKind && obj.Schema == target.schema && string(obj.Name) == target.name {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		case schema.CompositeDef:
			if target.kind == schema.TypeKind && obj.Schema == target.schema && string(obj.Name) == target.name {
				obj.Grants = mutate(obj.Grants)
				objects[i] = obj
				matches++
			}
		}
	}
	if matches != 1 {
		return nil, metadataAttachmentError(statement, target, matches)
	}
	return objects, nil
}

func privilegesForGrant(stmt *pg_query.GrantStmt) ([]schema.Privilege, error) {
	allowed := allowedPrivileges(stmt.Objtype)
	if len(allowed) == 0 {
		return nil, fmt.Errorf("unsupported GRANT object type %s", stmt.Objtype)
	}
	if len(stmt.Privileges) == 0 { // PostgreSQL represents ALL as an empty list.
		if stmt.IsGrant && isTableGrantObject(stmt.Objtype) {
			return nil, fmt.Errorf("GRANT ALL on tables and views is PostgreSQL-version-dependent because MAINTAIN was added in PostgreSQL 17; list privileges explicitly")
		}
		return allPrivilegesForGrant(stmt.Objtype), nil
	}

	privs := make([]schema.Privilege, 0, len(stmt.Privileges))
	for _, node := range stmt.Privileges {
		if node == nil || node.GetAccessPriv() == nil {
			return nil, fmt.Errorf("GRANT contains an unsupported privilege AST node")
		}
		access := node.GetAccessPriv()
		if len(access.Cols) > 0 {
			return nil, fmt.Errorf("column-level privileges are not modeled; grant privileges on the complete object or use an explicit migration")
		}
		priv := schema.Privilege(strings.ToUpper(strings.TrimSpace(access.PrivName)))
		if !containsPrivilege(allowed, priv) {
			return nil, fmt.Errorf("privilege %q is not supported for GRANT object type %s", access.PrivName, stmt.Objtype)
		}
		privs = append(privs, priv)
	}
	return privs, nil
}

func allPrivilegesForGrant(objType pg_query.ObjectType) []schema.Privilege {
	switch objType {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW:
		// This table-family expansion is used only for REVOKE ALL. Include every
		// privilege known to the model so a preceding explicit MAINTAIN grant is
		// removed as well. GRANT ALL is rejected above because its meaning differs
		// between PostgreSQL 16 and 17+.
		return []schema.Privilege{schema.PrivSelect, schema.PrivInsert, schema.PrivUpdate, schema.PrivDelete, schema.PrivTruncate, schema.PrivReferences, schema.PrivTrigger, schema.PrivMaintain}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		return []schema.Privilege{schema.PrivUsage, schema.PrivSelect, schema.PrivUpdate}
	case pg_query.ObjectType_OBJECT_FUNCTION:
		return []schema.Privilege{schema.PrivExecute}
	case pg_query.ObjectType_OBJECT_TYPE, pg_query.ObjectType_OBJECT_DOMAIN:
		return []schema.Privilege{schema.PrivUsage}
	default:
		return nil
	}
}

func isTableGrantObject(objType pg_query.ObjectType) bool {
	switch objType {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW:
		return true
	default:
		return false
	}
}

func allowedPrivileges(objType pg_query.ObjectType) []schema.Privilege {
	switch objType {
	case pg_query.ObjectType_OBJECT_TABLE, pg_query.ObjectType_OBJECT_VIEW, pg_query.ObjectType_OBJECT_MATVIEW:
		return []schema.Privilege{schema.PrivSelect, schema.PrivInsert, schema.PrivUpdate, schema.PrivDelete, schema.PrivTruncate, schema.PrivReferences, schema.PrivTrigger, schema.PrivMaintain}
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		return []schema.Privilege{schema.PrivUsage, schema.PrivSelect, schema.PrivUpdate}
	case pg_query.ObjectType_OBJECT_FUNCTION:
		return []schema.Privilege{schema.PrivExecute}
	case pg_query.ObjectType_OBJECT_TYPE, pg_query.ObjectType_OBJECT_DOMAIN:
		return []schema.Privilege{schema.PrivUsage}
	default:
		return nil
	}
}

func containsPrivilege(values []schema.Privilege, value schema.Privilege) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func granteeFromNode(node *pg_query.Node) (schema.Grantee, error) {
	if node == nil || node.GetRoleSpec() == nil {
		return schema.Grantee{}, fmt.Errorf("GRANT contains an unsupported grantee")
	}
	role := node.GetRoleSpec()
	switch role.GetRoletype() {
	case pg_query.RoleSpecType_ROLESPEC_PUBLIC:
		return schema.PublicGrantee(), nil
	case pg_query.RoleSpecType_ROLESPEC_CSTRING:
		if role.GetRolename() == "" {
			return schema.Grantee{}, fmt.Errorf("GRANT contains an empty role name")
		}
		return schema.RoleGrantee(role.GetRolename()), nil
	default:
		return schema.Grantee{}, fmt.Errorf("context-dependent GRANT grantee %s is not declarative; use an explicit role name", role.GetRoletype())
	}
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

func listNodeStrings(nodes []*pg_query.Node) []string {
	values := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node != nil && node.GetString_() != nil {
			values = append(values, node.GetString_().Sval)
		}
	}
	return values
}

func (p *Parser) mergeAlterOwnerStmt(objects []schema.DatabaseObject, stmt *pg_query.AlterOwnerStmt) ([]schema.DatabaseObject, error) {
	if stmt == nil || stmt.Newowner == nil {
		return nil, fmt.Errorf("ALTER OWNER is missing a new owner")
	}
	if stmt.Newowner.GetRoletype() != pg_query.RoleSpecType_ROLESPEC_CSTRING || stmt.Newowner.GetRolename() == "" {
		return nil, fmt.Errorf("context-dependent ALTER OWNER target %s is not declarative; use an explicit role name", stmt.Newowner.GetRoletype())
	}
	newOwner := stmt.Newowner.GetRolename()

	var target metadataTarget
	var err error
	switch stmt.ObjectType {
	case pg_query.ObjectType_OBJECT_TABLE:
		target.schema, target.name = extractRangeVarName(stmt.Relation)
		target.kind = schema.TableKind
	case pg_query.ObjectType_OBJECT_VIEW:
		target.schema, target.name = extractRangeVarName(stmt.Relation)
		target.kind = schema.ViewKind
		viewType := schema.RegularView
		target.viewType = &viewType
	case pg_query.ObjectType_OBJECT_MATVIEW:
		target.schema, target.name = extractRangeVarName(stmt.Relation)
		target.kind = schema.ViewKind
		viewType := schema.MaterializedView
		target.viewType = &viewType
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		target.schema, target.name = extractRangeVarName(stmt.Relation)
		target.kind = schema.SequenceKind
	case pg_query.ObjectType_OBJECT_FUNCTION:
		target.schema, target.name, target.signature, err = p.objectWithArgsIdentity(stmt.Object)
		target.kind = schema.FunctionKind
	case pg_query.ObjectType_OBJECT_TYPE, pg_query.ObjectType_OBJECT_DOMAIN:
		target.schema, target.name, err = qualifiedNameNode(stmt.Object)
		target.kind = schema.TypeKind
	default:
		return nil, fmt.Errorf("unsupported ALTER OWNER object type %s", stmt.ObjectType)
	}
	if err != nil {
		return nil, err
	}
	if target.name == "" {
		return nil, fmt.Errorf("ALTER OWNER target is missing an object name")
	}

	matches := 0
	for i := range objects {
		if !metadataTargetMatches(target, objects[i]) {
			continue
		}
		switch obj := objects[i].(type) {
		case schema.Table:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		case schema.View:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		case schema.Sequence:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		case schema.Function:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		case schema.EnumDef:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		case schema.DomainDef:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		case schema.CompositeDef:
			obj.Owner = strPtr(newOwner)
			objects[i] = obj
		}
		matches++
	}
	if matches != 1 {
		return nil, metadataAttachmentError("ALTER OWNER", target, matches)
	}
	return objects, nil
}

func isOwnerOnlyAlterTableStmt(stmt *pg_query.AlterTableStmt) bool {
	if stmt == nil || stmt.Relation == nil || len(stmt.Cmds) != 1 {
		return false
	}
	cmd := stmt.Cmds[0]
	return cmd != nil && cmd.GetAlterTableCmd() != nil && cmd.GetAlterTableCmd().Subtype == pg_query.AlterTableType_AT_ChangeOwner
}

func isOwnedByOnlyAlterSequenceStmt(stmt *pg_query.AlterSeqStmt) bool {
	if stmt == nil || stmt.Sequence == nil || len(stmt.Options) != 1 || stmt.Options[0] == nil || stmt.Options[0].GetDefElem() == nil {
		return false
	}
	return strings.EqualFold(stmt.Options[0].GetDefElem().Defname, "owned_by")
}

func (p *Parser) mergeAlterSequenceOwnedByStmt(objects []schema.DatabaseObject, stmt *pg_query.AlterSeqStmt) ([]schema.DatabaseObject, error) {
	if !isOwnedByOnlyAlterSequenceStmt(stmt) {
		return nil, fmt.Errorf("ALTER SEQUENCE metadata must contain exactly one OWNED BY option")
	}
	targetSchema, targetName := extractRangeVarName(stmt.Sequence)
	option := stmt.Options[0].GetDefElem()
	names := extractStringList(option.Arg)
	if len(names) == 1 && strings.EqualFold(names[0], "none") {
		return nil, fmt.Errorf("ALTER SEQUENCE OWNED BY NONE cannot be represented as an explicit desired removal")
	}
	if len(names) != 2 && len(names) != 3 {
		return nil, fmt.Errorf("ALTER SEQUENCE OWNED BY must name table.column or schema.table.column")
	}
	ownerSchema := targetSchema
	if len(names) == 3 {
		ownerSchema = schema.SchemaName(names[0])
	}
	ownedBy := &schema.SequenceOwner{Schema: ownerSchema, Table: schema.TableName(names[len(names)-2]), Column: schema.ColumnName(names[len(names)-1])}

	matches := 0
	for i := range objects {
		sequence, ok := objects[i].(schema.Sequence)
		if !ok || sequence.Schema != targetSchema || sequence.Name != targetName {
			continue
		}
		sequence.OwnedBy = ownedBy
		objects[i] = sequence
		matches++
	}
	if matches != 1 {
		return nil, metadataAttachmentError("ALTER SEQUENCE OWNED BY", metadataTarget{kind: schema.SequenceKind, schema: targetSchema, name: targetName}, matches)
	}
	return objects, nil
}

func (p *Parser) mergeAlterTableOwnerStmt(objects []schema.DatabaseObject, stmt *pg_query.AlterTableStmt) ([]schema.DatabaseObject, error) {
	if !isOwnerOnlyAlterTableStmt(stmt) {
		return nil, fmt.Errorf("ALTER relation metadata must contain exactly one OWNER command")
	}
	cmd := stmt.Cmds[0].GetAlterTableCmd()
	if cmd.Newowner == nil || cmd.Newowner.GetRoletype() != pg_query.RoleSpecType_ROLESPEC_CSTRING || cmd.Newowner.GetRolename() == "" {
		roleType := pg_query.RoleSpecType_ROLE_SPEC_TYPE_UNDEFINED
		if cmd.Newowner != nil {
			roleType = cmd.Newowner.GetRoletype()
		}
		return nil, fmt.Errorf("context-dependent ALTER OWNER target %s is not declarative; use an explicit role name", roleType)
	}

	sn, name := extractRangeVarName(stmt.Relation)
	target := metadataTarget{schema: sn, name: name}
	switch stmt.Objtype {
	case pg_query.ObjectType_OBJECT_TABLE:
		target.kind = schema.TableKind
	case pg_query.ObjectType_OBJECT_VIEW:
		target.kind = schema.ViewKind
		viewType := schema.RegularView
		target.viewType = &viewType
	case pg_query.ObjectType_OBJECT_MATVIEW:
		target.kind = schema.ViewKind
		viewType := schema.MaterializedView
		target.viewType = &viewType
	case pg_query.ObjectType_OBJECT_SEQUENCE:
		target.kind = schema.SequenceKind
	default:
		return nil, fmt.Errorf("unsupported ALTER OWNER relation type %s", stmt.Objtype)
	}

	matches := 0
	for i := range objects {
		if !metadataTargetMatches(target, objects[i]) {
			continue
		}
		owner := strPtr(cmd.Newowner.GetRolename())
		switch obj := objects[i].(type) {
		case schema.Table:
			obj.Owner = owner
			objects[i] = obj
		case schema.View:
			obj.Owner = owner
			objects[i] = obj
		case schema.Sequence:
			obj.Owner = owner
			objects[i] = obj
		}
		matches++
	}
	if matches != 1 {
		return nil, metadataAttachmentError("ALTER OWNER", target, matches)
	}
	return objects, nil
}

func metadataTargetMatches(target metadataTarget, object schema.DatabaseObject) bool {
	key := objectmap.Key(object)
	if key.Kind != target.kind || key.Schema != target.schema || key.Name != target.name {
		// OBJECT_TABLE grants are also the SQL surface for views.
		if !(target.tableGrantSurface && key.Kind == schema.ViewKind && key.Schema == target.schema && key.Name == target.name) {
			return false
		}
	}
	if target.signature != "" && key.Signature != target.signature {
		return false
	}
	if target.viewType != nil {
		view, ok := object.(schema.View)
		return ok && view.Type == *target.viewType
	}
	return true
}

func metadataAttachmentError(statement string, target metadataTarget, matches int) error {
	if matches == 0 {
		return fmt.Errorf("%s target %s was not defined in the schema input", statement, target)
	}
	return fmt.Errorf("%s target %s is ambiguous: matched %d modeled objects", statement, target, matches)
}

func strPtr(s string) *string { return &s }

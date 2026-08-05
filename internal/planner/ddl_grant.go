package planner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

func joinPrivileges(privs []schema.Privilege) string {
	canonical := append([]schema.Privilege(nil), privs...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i] < canonical[j] })
	parts := make([]string, len(canonical))
	for i := range canonical {
		parts[i] = string(canonical[i])
	}
	return strings.Join(parts, ", ")
}

func formatTableGrant(tbl schema.Table, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON TABLE %s TO %s",
		joinPrivileges(privs), qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedGrantee(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatTableRevoke(tbl schema.Table, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TABLE %s FROM %s;",
			joinPrivileges(privs), qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedGrantee(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON TABLE %s FROM %s;",
		joinPrivileges(privs), qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedGrantee(grantee))
}

func formatViewGrant(v schema.View, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON TABLE %s TO %s",
		joinPrivileges(privs), qualifiedName(string(v.Schema), v.Name), quotedGrantee(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatViewRevoke(v schema.View, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TABLE %s FROM %s;",
			joinPrivileges(privs), qualifiedName(string(v.Schema), v.Name), quotedGrantee(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON TABLE %s FROM %s;",
		joinPrivileges(privs), qualifiedName(string(v.Schema), v.Name), quotedGrantee(grantee))
}

func formatSequenceGrant(seq schema.Sequence, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON SEQUENCE %s TO %s",
		joinPrivileges(privs), qualifiedName(string(seq.Schema), seq.Name), quotedGrantee(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatSequenceRevoke(seq schema.Sequence, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON SEQUENCE %s FROM %s;",
			joinPrivileges(privs), qualifiedName(string(seq.Schema), seq.Name), quotedGrantee(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON SEQUENCE %s FROM %s;",
		joinPrivileges(privs), qualifiedName(string(seq.Schema), seq.Name), quotedGrantee(grantee))
}

func formatFunctionGrant(fn schema.Function, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	sig := functionSignatureForACL(fn)
	stmt := fmt.Sprintf("GRANT %s ON FUNCTION %s TO %s",
		joinPrivileges(privs), sig, quotedGrantee(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatFunctionRevoke(fn schema.Function, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	sig := functionSignatureForACL(fn)
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON FUNCTION %s FROM %s;",
			joinPrivileges(privs), sig, quotedGrantee(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON FUNCTION %s FROM %s;",
		joinPrivileges(privs), sig, quotedGrantee(grantee))
}

func functionSignatureForACL(fn schema.Function) string {
	identityTypes := make([]string, 0, len(fn.Args))
	for _, arg := range fn.Args {
		if arg.Mode == schema.OutMode || arg.Mode == schema.TableMode {
			continue
		}
		raw := strings.TrimSpace(string(arg.Type))
		lower := strings.ToLower(raw)
		if strings.HasPrefix(lower, "public.") || strings.HasPrefix(lower, `"public".`) {
			identityTypes = append(identityTypes, raw)
		} else {
			identityTypes = append(identityTypes, string(schema.NormalizeTypeName(arg.Type)))
		}
	}
	return qualifiedName(string(fn.Schema), fn.Name) + "(" + strings.Join(identityTypes, ",") + ")"
}

func onlyOwnerOrGrantChanges(changes []string) bool {
	for _, c := range changes {
		if c != "owner changed" && c != "comment changed" && !strings.HasPrefix(c, "add grant\t") && !strings.HasPrefix(c, "revoke grant\t") {
			return false
		}
	}
	return true
}

// grantStatementsFromView emits GRANT statements for all privileges on a view.
func grantStatementsFromView(v schema.View) []string {
	var out []string
	for _, gr := range schema.CanonicalizeGrants(v.Grants) {
		out = append(out, formatViewGrant(v, gr.Grantee, gr.Privileges, gr.Grantable))
	}
	return out
}

func formatTypeGrant(schemaName schema.SchemaName, name schema.TypeName, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON TYPE %s TO %s", joinPrivileges(privs), qualifiedName(string(schemaName), string(name)), quotedGrantee(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatTypeRevoke(schemaName schema.SchemaName, name schema.TypeName, grantee schema.Grantee, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TYPE %s FROM %s;", joinPrivileges(privs), qualifiedName(string(schemaName), string(name)), quotedGrantee(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON TYPE %s FROM %s;", joinPrivileges(privs), qualifiedName(string(schemaName), string(name)), quotedGrantee(grantee))
}

// managedACLReset clears PostgreSQL's object defaults before applying a
// non-nil authoritative grant set. Owner privileges are ACL entries even
// though ownership independently retains all owner capabilities.
func managedACLReset(objectSQL string, owner *string, priorGrants ...schema.Grant) []string {
	ownerSQL := "CURRENT_USER"
	if owner != nil {
		ownerSQL = quotedRole(*owner)
	}
	out := []string{
		fmt.Sprintf("REVOKE ALL ON %s FROM PUBLIC;", objectSQL),
		fmt.Sprintf("REVOKE ALL ON %s FROM %s;", objectSQL, ownerSQL),
	}

	// ALTER OWNER preserves ACL entries for arbitrary grantees and rewrites
	// their grantor to the new owner. Clear every grantee observed in the prior
	// catalog ACL before rebuilding an authoritative desired ACL, otherwise a
	// stale grant survives the ownership transfer.
	seen := map[schema.Grantee]struct{}{schema.PublicGrantee(): {}}
	if owner != nil {
		seen[schema.RoleGrantee(*owner)] = struct{}{}
	}
	prior := schema.CanonicalizeGrants(priorGrants)
	for _, grant := range prior {
		if _, exists := seen[grant.Grantee]; exists {
			continue
		}
		seen[grant.Grantee] = struct{}{}
		out = append(out, fmt.Sprintf("REVOKE ALL ON %s FROM %s;", objectSQL, quotedGrantee(grant.Grantee)))
	}
	return out
}

func tableCreateMetadata(tbl schema.Table) []string {
	target := "TABLE " + qualifiedName(string(tbl.Schema), string(tbl.Name))
	var out []string
	if tbl.Owner != nil {
		out = append(out, fmt.Sprintf("ALTER TABLE %s OWNER TO %s;", qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedRole(*tbl.Owner)))
	}
	if tbl.Comment != nil {
		out = append(out, formatTableCommentStatement(tbl.Schema, tbl.Name, tbl.Comment))
	}
	for _, column := range tbl.Columns {
		if column.Comment != nil {
			out = append(out, formatColumnCommentStatement(tbl.Schema, tbl.Name, column.Name, column.Comment))
		}
	}
	if tbl.Grants != nil {
		out = append(out, managedACLReset(target, tbl.Owner)...)
		for _, grant := range schema.CanonicalizeGrants(tbl.Grants) {
			out = append(out, formatTableGrant(tbl, grant.Grantee, grant.Privileges, grant.Grantable))
		}
	}
	return out
}

func viewCreateMetadata(view schema.View) []string {
	keyword := viewAlterKeyword(view)
	target := "TABLE " + qualifiedName(string(view.Schema), view.Name)
	var out []string
	if view.Owner != nil {
		out = append(out, fmt.Sprintf("ALTER %s %s OWNER TO %s;", keyword, qualifiedName(string(view.Schema), view.Name), quotedRole(*view.Owner)))
	}
	if view.Comment != nil {
		out = append(out, fmt.Sprintf("COMMENT ON %s %s IS %s;", keyword, qualifiedName(string(view.Schema), view.Name), quotedLiteral(*view.Comment)))
	}
	if view.Grants != nil {
		out = append(out, managedACLReset(target, view.Owner)...)
		out = append(out, grantStatementsFromView(view)...)
	}
	return out
}

func sequenceCreateMetadata(sequence schema.Sequence) []string {
	qualified := qualifiedName(string(sequence.Schema), sequence.Name)
	var out []string
	if sequence.Owner != nil {
		out = append(out, fmt.Sprintf("ALTER SEQUENCE %s OWNER TO %s;", qualified, quotedRole(*sequence.Owner)))
	}
	if sequence.OwnedBy != nil {
		out = append(out, fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s;", qualified, qualifiedColumnName(string(sequence.OwnedBy.Schema), string(sequence.OwnedBy.Table), string(sequence.OwnedBy.Column))))
	}
	if sequence.Comment != nil {
		out = append(out, fmt.Sprintf("COMMENT ON SEQUENCE %s IS %s;", qualified, quotedLiteral(*sequence.Comment)))
	}
	if sequence.Grants != nil {
		out = append(out, managedACLReset("SEQUENCE "+qualified, sequence.Owner)...)
		for _, grant := range schema.CanonicalizeGrants(sequence.Grants) {
			out = append(out, formatSequenceGrant(sequence, grant.Grantee, grant.Privileges, grant.Grantable))
		}
	}
	return out
}

func functionCreateMetadata(function schema.Function) []string {
	signature := functionSignatureForACL(function)
	var out []string
	if function.Owner != nil {
		out = append(out, fmt.Sprintf("ALTER FUNCTION %s OWNER TO %s;", signature, quotedRole(*function.Owner)))
	}
	if function.Comment != nil {
		out = append(out, fmt.Sprintf("COMMENT ON FUNCTION %s IS %s;", signature, quotedLiteral(*function.Comment)))
	}
	if function.Grants != nil {
		out = append(out, managedACLReset("FUNCTION "+signature, function.Owner)...)
		for _, grant := range schema.CanonicalizeGrants(function.Grants) {
			out = append(out, formatFunctionGrant(function, grant.Grantee, grant.Privileges, grant.Grantable))
		}
	}
	return out
}

func typeCreateMetadata(objectKeyword string, schemaName schema.SchemaName, name schema.TypeName, owner *string, comment *string, grants []schema.Grant) []string {
	qualified := qualifiedName(string(schemaName), string(name))
	var out []string
	if owner != nil {
		out = append(out, fmt.Sprintf("ALTER %s %s OWNER TO %s;", objectKeyword, qualified, quotedRole(*owner)))
	}
	if comment != nil {
		out = append(out, fmt.Sprintf("COMMENT ON %s %s IS %s;", objectKeyword, qualified, quotedLiteral(*comment)))
	}
	if grants != nil {
		out = append(out, managedACLReset("TYPE "+qualified, owner)...)
		for _, grant := range schema.CanonicalizeGrants(grants) {
			out = append(out, formatTypeGrant(schemaName, name, grant.Grantee, grant.Privileges, grant.Grantable))
		}
	}
	return out
}

func appendMetadata(create string, metadata []string) string {
	if len(metadata) == 0 {
		return create
	}
	return create + "\n\n" + strings.Join(metadata, "\n")
}

func hasAlterChange(changes []string, wanted string) bool {
	for _, change := range changes {
		if change == wanted {
			return true
		}
	}
	return false
}

func tableManagedACLStatements(table schema.Table, priorGrants []schema.Grant) []string {
	out := managedACLReset("TABLE "+qualifiedName(string(table.Schema), string(table.Name)), table.Owner, priorGrants...)
	for _, grant := range schema.CanonicalizeGrants(table.Grants) {
		out = append(out, formatTableGrant(table, grant.Grantee, grant.Privileges, grant.Grantable))
	}
	return out
}

func viewManagedACLStatements(view schema.View, priorGrants []schema.Grant) []string {
	out := managedACLReset("TABLE "+qualifiedName(string(view.Schema), view.Name), view.Owner, priorGrants...)
	return append(out, grantStatementsFromView(view)...)
}

func sequenceManagedACLStatements(sequence schema.Sequence, priorGrants []schema.Grant) []string {
	out := managedACLReset("SEQUENCE "+qualifiedName(string(sequence.Schema), sequence.Name), sequence.Owner, priorGrants...)
	for _, grant := range schema.CanonicalizeGrants(sequence.Grants) {
		out = append(out, formatSequenceGrant(sequence, grant.Grantee, grant.Privileges, grant.Grantable))
	}
	return out
}

func functionManagedACLStatements(function schema.Function, priorGrants []schema.Grant) []string {
	out := managedACLReset("FUNCTION "+functionSignatureForACL(function), function.Owner, priorGrants...)
	for _, grant := range schema.CanonicalizeGrants(function.Grants) {
		out = append(out, formatFunctionGrant(function, grant.Grantee, grant.Privileges, grant.Grantable))
	}
	return out
}

func typeManagedACLStatements(schemaName schema.SchemaName, name schema.TypeName, owner *string, grants, priorGrants []schema.Grant) []string {
	out := managedACLReset("TYPE "+qualifiedName(string(schemaName), string(name)), owner, priorGrants...)
	for _, grant := range schema.CanonicalizeGrants(grants) {
		out = append(out, formatTypeGrant(schemaName, name, grant.Grantee, grant.Privileges, grant.Grantable))
	}
	return out
}

func viewAlterKeyword(v schema.View) string {
	if v.Type == schema.MaterializedView {
		return "MATERIALIZED VIEW"
	}
	return "VIEW"
}

func (g *DDLGenerator) generateAlterViewOwnerAndGrants(v schema.View, alter differ.AlterOperation) ([]string, error) {
	var stmts []string
	kw := viewAlterKeyword(v)
	replaceACL := v.Grants != nil && hasAlterChange(alter.Changes, "owner changed")
	var priorGrants []schema.Grant
	if replaceACL {
		old, ok := alter.OldObject.(schema.View)
		if !ok {
			return nil, fmt.Errorf("cannot rebuild managed view ACL without the prior view definition")
		}
		priorGrants = old.Grants
	}
	for _, ch := range alter.Changes {
		if ch == "owner changed" && v.Owner != nil {
			stmts = append(stmts, fmt.Sprintf("ALTER %s %s OWNER TO %s;", kw,
				qualifiedName(string(v.Schema), v.Name), quotedRole(*v.Owner)))
		} else if ch == "comment changed" {
			if v.Comment == nil {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON %s %s IS NULL;", kw, qualifiedName(string(v.Schema), v.Name)))
			} else {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON %s %s IS %s;", kw, qualifiedName(string(v.Schema), v.Name), quotedLiteral(*v.Comment)))
			}
		} else if strings.HasPrefix(ch, "add grant\t") || strings.HasPrefix(ch, "revoke grant\t") {
			if replaceACL {
				continue
			}
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(ch)
			if !ok {
				return nil, fmt.Errorf("invalid encoded view grant change %q", ch)
			}
			if revoke {
				stmts = append(stmts, formatViewRevoke(v, grantee, privs, grantable))
			} else {
				stmts = append(stmts, formatViewGrant(v, grantee, privs, grantable))
			}
		}
	}
	if replaceACL {
		stmts = append(stmts, viewManagedACLStatements(v, priorGrants)...)
	}
	return stmts, nil
}

func (g *DDLGenerator) generateAlterSequenceOwnerAndGrants(seq schema.Sequence, alter differ.AlterOperation, ownerCascaded bool) ([]string, error) {
	var stmts []string
	replaceACL := seq.Grants != nil && hasAlterChange(alter.Changes, "owner changed")
	var priorGrants []schema.Grant
	if replaceACL {
		old, ok := alter.OldObject.(schema.Sequence)
		if !ok {
			return nil, fmt.Errorf("cannot rebuild managed sequence ACL without the prior sequence definition")
		}
		priorGrants = old.Grants
	}
	for _, ch := range alter.Changes {
		if ch == "owner changed" && seq.Owner != nil && !ownerCascaded {
			stmts = append(stmts, fmt.Sprintf("ALTER SEQUENCE %s OWNER TO %s;",
				qualifiedName(string(seq.Schema), seq.Name), quotedRole(*seq.Owner)))
		} else if ch == "comment changed" {
			qualified := qualifiedName(string(seq.Schema), seq.Name)
			if seq.Comment == nil {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON SEQUENCE %s IS NULL;", qualified))
			} else {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON SEQUENCE %s IS %s;", qualified, quotedLiteral(*seq.Comment)))
			}
		} else if strings.HasPrefix(ch, "add grant\t") || strings.HasPrefix(ch, "revoke grant\t") {
			if replaceACL {
				continue
			}
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(ch)
			if !ok {
				return nil, fmt.Errorf("invalid encoded sequence grant change %q", ch)
			}
			if revoke {
				stmts = append(stmts, formatSequenceRevoke(seq, grantee, privs, grantable))
			} else {
				stmts = append(stmts, formatSequenceGrant(seq, grantee, privs, grantable))
			}
		}
	}
	if replaceACL {
		stmts = append(stmts, sequenceManagedACLStatements(seq, priorGrants)...)
	}
	return stmts, nil
}

func (g *DDLGenerator) generateAlterFunction(fn schema.Function, alter differ.AlterOperation) ([]string, error) {
	var stmts []string
	needReplace := false
	replaceACL := fn.Grants != nil && hasAlterChange(alter.Changes, "owner changed")
	var priorGrants []schema.Grant
	if replaceACL {
		old, ok := alter.OldObject.(schema.Function)
		if !ok {
			return nil, fmt.Errorf("cannot rebuild managed function ACL without the prior function definition")
		}
		priorGrants = old.Grants
	}
	for _, ch := range alter.Changes {
		if ch == "return type changed" || ch == "arguments changed" {
			return nil, &UnsupportedChangeError{
				Key:         alter.Key,
				Change:      ch,
				Remediation: "PostgreSQL cannot apply this change with CREATE OR REPLACE FUNCTION; use an explicit drop/create migration after reviewing dependents",
			}
		}
		if ch != "owner changed" &&
			ch != "comment changed" &&
			!strings.HasPrefix(ch, "add grant\t") &&
			!strings.HasPrefix(ch, "revoke grant\t") {
			needReplace = true
		}
	}
	if needReplace {
		stmts = append(stmts, g.renderFunction(fn, true))
	}

	for _, ch := range alter.Changes {
		if ch == "owner changed" && fn.Owner != nil {
			stmts = append(stmts, fmt.Sprintf("ALTER FUNCTION %s OWNER TO %s;", functionSignatureForACL(fn), quotedRole(*fn.Owner)))
		} else if strings.HasPrefix(ch, "add grant\t") || strings.HasPrefix(ch, "revoke grant\t") {
			if replaceACL {
				continue
			}
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(ch)
			if !ok {
				return nil, fmt.Errorf("invalid encoded function grant change %q", ch)
			}
			if revoke {
				stmts = append(stmts, formatFunctionRevoke(fn, grantee, privs, grantable))
			} else {
				stmts = append(stmts, formatFunctionGrant(fn, grantee, privs, grantable))
			}
		}
	}
	if replaceACL {
		stmts = append(stmts, functionManagedACLStatements(fn, priorGrants)...)
	}
	for _, ch := range alter.Changes {
		if ch == "comment changed" {
			if fn.Comment == nil {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON FUNCTION %s IS NULL;", functionSignatureForACL(fn)))
			} else {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON FUNCTION %s IS %s;", functionSignatureForACL(fn), quotedLiteral(*fn.Comment)))
			}
		}
	}
	return stmts, nil
}

func (g *DDLGenerator) generateAlterTypeMetadata(
	objectKeyword string,
	schemaName schema.SchemaName,
	name schema.TypeName,
	owner *string,
	comment *string,
	grants []schema.Grant,
	alter differ.AlterOperation,
) ([]string, error) {
	replaceACL := grants != nil && hasAlterChange(alter.Changes, "owner changed")
	var priorGrants []schema.Grant
	if replaceACL {
		var ok bool
		priorGrants, ok = grantsFromTypeObject(alter.OldObject)
		if !ok {
			return nil, fmt.Errorf("cannot rebuild managed %s ACL without the prior type definition", strings.ToLower(objectKeyword))
		}
	}

	qualified := qualifiedName(string(schemaName), string(name))
	var stmts []string
	for _, change := range alter.Changes {
		switch {
		case change == "owner changed" && owner != nil:
			stmts = append(stmts, fmt.Sprintf("ALTER %s %s OWNER TO %s;", objectKeyword, qualified, quotedRole(*owner)))
		case change == "comment changed":
			if comment == nil {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON %s %s IS NULL;", objectKeyword, qualified))
			} else {
				stmts = append(stmts, fmt.Sprintf("COMMENT ON %s %s IS %s;", objectKeyword, qualified, quotedLiteral(*comment)))
			}
		case strings.HasPrefix(change, "add grant\t") || strings.HasPrefix(change, "revoke grant\t"):
			if replaceACL {
				continue
			}
			revoke, grantee, privileges, grantable, ok := differ.ParseGrantChange(change)
			if !ok {
				return nil, fmt.Errorf("invalid encoded type grant change %q", change)
			}
			if revoke {
				stmts = append(stmts, formatTypeRevoke(schemaName, name, grantee, privileges, grantable))
			} else {
				stmts = append(stmts, formatTypeGrant(schemaName, name, grantee, privileges, grantable))
			}
		}
	}
	if replaceACL {
		stmts = append(stmts, typeManagedACLStatements(schemaName, name, owner, grants, priorGrants)...)
	}
	return stmts, nil
}

func grantsFromTypeObject(object schema.DatabaseObject) ([]schema.Grant, bool) {
	switch value := object.(type) {
	case schema.EnumDef:
		return value.Grants, true
	case schema.DomainDef:
		return value.Grants, true
	case schema.CompositeDef:
		return value.Grants, true
	default:
		return nil, false
	}
}

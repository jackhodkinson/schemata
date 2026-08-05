package planner

import (
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/internal/differ"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

func joinPrivileges(privs []schema.Privilege) string {
	parts := make([]string, len(privs))
	for i := range privs {
		parts[i] = string(privs[i])
	}
	return strings.Join(parts, ", ")
}

func formatTableGrant(tbl schema.Table, grantee string, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON TABLE %s TO %s",
		joinPrivileges(privs), qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedRole(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatTableRevoke(tbl schema.Table, grantee string, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TABLE %s FROM %s;",
			joinPrivileges(privs), qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedRole(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON TABLE %s FROM %s;",
		joinPrivileges(privs), qualifiedName(string(tbl.Schema), string(tbl.Name)), quotedRole(grantee))
}

func formatViewGrant(v schema.View, grantee string, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON TABLE %s TO %s",
		joinPrivileges(privs), qualifiedName(string(v.Schema), v.Name), quotedRole(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatViewRevoke(v schema.View, grantee string, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON TABLE %s FROM %s;",
			joinPrivileges(privs), qualifiedName(string(v.Schema), v.Name), quotedRole(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON TABLE %s FROM %s;",
		joinPrivileges(privs), qualifiedName(string(v.Schema), v.Name), quotedRole(grantee))
}

func formatSequenceGrant(seq schema.Sequence, grantee string, privs []schema.Privilege, grantable bool) string {
	stmt := fmt.Sprintf("GRANT %s ON SEQUENCE %s TO %s",
		joinPrivileges(privs), qualifiedName(string(seq.Schema), seq.Name), quotedRole(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatSequenceRevoke(seq schema.Sequence, grantee string, privs []schema.Privilege, grantable bool) string {
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON SEQUENCE %s FROM %s;",
			joinPrivileges(privs), qualifiedName(string(seq.Schema), seq.Name), quotedRole(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON SEQUENCE %s FROM %s;",
		joinPrivileges(privs), qualifiedName(string(seq.Schema), seq.Name), quotedRole(grantee))
}

func formatFunctionGrant(fn schema.Function, grantee string, privs []schema.Privilege, grantable bool) string {
	sig := functionSignatureForACL(fn)
	stmt := fmt.Sprintf("GRANT %s ON FUNCTION %s TO %s",
		joinPrivileges(privs), sig, quotedRole(grantee))
	if grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt + ";"
}

func formatFunctionRevoke(fn schema.Function, grantee string, privs []schema.Privilege, grantable bool) string {
	sig := functionSignatureForACL(fn)
	if grantable {
		return fmt.Sprintf("REVOKE GRANT OPTION FOR %s ON FUNCTION %s FROM %s;",
			joinPrivileges(privs), sig, quotedRole(grantee))
	}
	return fmt.Sprintf("REVOKE %s ON FUNCTION %s FROM %s;",
		joinPrivileges(privs), sig, quotedRole(grantee))
}

func functionSignatureForACL(fn schema.Function) string {
	var args []string
	for _, a := range fn.Args {
		args = append(args, string(a.Type))
	}
	return fmt.Sprintf("%s(%s)", qualifiedName(string(fn.Schema), fn.Name), strings.Join(args, ", "))
}

func onlyOwnerOrGrantChanges(changes []string) bool {
	for _, c := range changes {
		if c != "owner changed" && !strings.HasPrefix(c, "add grant\t") && !strings.HasPrefix(c, "revoke grant\t") {
			return false
		}
	}
	return true
}

// grantStatementsFromView emits GRANT statements for all privileges on a view.
func grantStatementsFromView(v schema.View) []string {
	var out []string
	for _, gr := range v.Grants {
		out = append(out, formatViewGrant(v, gr.Grantee, gr.Privileges, gr.Grantable))
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
	for _, ch := range alter.Changes {
		if ch == "owner changed" && v.Owner != nil {
			stmts = append(stmts, fmt.Sprintf("ALTER %s %s OWNER TO %s;", kw,
				qualifiedName(string(v.Schema), v.Name), quotedRole(*v.Owner)))
		} else if strings.HasPrefix(ch, "add grant\t") || strings.HasPrefix(ch, "revoke grant\t") {
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(ch)
			if ok {
				if revoke {
					stmts = append(stmts, formatViewRevoke(v, grantee, privs, grantable))
				} else {
					stmts = append(stmts, formatViewGrant(v, grantee, privs, grantable))
				}
			}
		}
	}
	return stmts, nil
}

func (g *DDLGenerator) generateAlterSequenceOwnerAndGrants(seq schema.Sequence, alter differ.AlterOperation) ([]string, error) {
	var stmts []string
	for _, ch := range alter.Changes {
		if ch == "owner changed" && seq.Owner != nil {
			stmts = append(stmts, fmt.Sprintf("ALTER SEQUENCE %s OWNER TO %s;",
				qualifiedName(string(seq.Schema), seq.Name), quotedRole(*seq.Owner)))
		} else if strings.HasPrefix(ch, "add grant\t") || strings.HasPrefix(ch, "revoke grant\t") {
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(ch)
			if ok {
				if revoke {
					stmts = append(stmts, formatSequenceRevoke(seq, grantee, privs, grantable))
				} else {
					stmts = append(stmts, formatSequenceGrant(seq, grantee, privs, grantable))
				}
			}
		}
	}
	return stmts, nil
}

func (g *DDLGenerator) generateAlterFunction(fn schema.Function, alter differ.AlterOperation) ([]string, error) {
	var stmts []string
	needReplace := false
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
			revoke, grantee, privs, grantable, ok := differ.ParseGrantChange(ch)
			if ok {
				if revoke {
					stmts = append(stmts, formatFunctionRevoke(fn, grantee, privs, grantable))
				} else {
					stmts = append(stmts, formatFunctionGrant(fn, grantee, privs, grantable))
				}
			}
		}
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

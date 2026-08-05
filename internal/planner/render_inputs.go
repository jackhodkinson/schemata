package planner

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/sqlrender"
	"github.com/jackhodkinson/schemata/pkg/schema"
)

func validateObjectKeyRenderInputs(key schema.ObjectKey) error {
	if key.Kind == "" {
		return fmt.Errorf("cannot render object key without a kind")
	}
	if err := validateIdentifier("schema", string(key.Schema)); err != nil {
		return err
	}
	if err := validateIdentifier("object", key.Name); err != nil {
		return err
	}
	if key.TableName != "" {
		if err := validateIdentifier("table", string(key.TableName)); err != nil {
			return err
		}
	}
	if key.ColumnName != "" {
		if err := validateIdentifier("column", string(key.ColumnName)); err != nil {
			return err
		}
	}
	return nil
}

func validateDatabaseObjectRenderInputs(obj schema.DatabaseObject) error {
	if obj == nil {
		return fmt.Errorf("cannot render a nil database object")
	}

	validateOwner := func(owner *string) error {
		if owner == nil {
			return nil
		}
		_, err := sqlrender.Role(*owner)
		return err
	}
	validateGrants := func(grants []schema.Grant) error {
		for _, grant := range grants {
			if err := validateGrantRenderInputs(grant.Grantee, grant.Privileges); err != nil {
				return err
			}
		}
		return nil
	}

	switch value := obj.(type) {
	case schema.Schema:
		if err := validateIdentifier("schema", string(value.Name)); err != nil {
			return err
		}
		return validateOwner(value.Owner)
	case schema.Extension:
		if err := validateIdentifier("schema", string(value.Schema)); err != nil {
			return err
		}
		return validateIdentifier("extension", value.Name)
	case schema.EnumDef:
		if err := validateQualifiedObject(string(value.Schema), string(value.Name)); err != nil {
			return err
		}
		for _, enumValue := range value.Values {
			if _, err := sqlrender.Literal(enumValue); err != nil {
				return err
			}
		}
		return validateOptionalLiteral(value.Comment)
	case schema.DomainDef:
		if err := validateQualifiedObject(string(value.Schema), string(value.Name)); err != nil {
			return err
		}
		return validateOptionalLiteral(value.Comment)
	case schema.CompositeDef:
		if err := validateQualifiedObject(string(value.Schema), string(value.Name)); err != nil {
			return err
		}
		for _, attr := range value.Attributes {
			if err := validateIdentifier("composite attribute", attr.Name); err != nil {
				return err
			}
		}
		return validateOptionalLiteral(value.Comment)
	case schema.Sequence:
		if err := validateQualifiedObject(string(value.Schema), value.Name); err != nil {
			return err
		}
		if err := validateOwner(value.Owner); err != nil {
			return err
		}
		if value.OwnedBy != nil {
			if err := validateIdentifier("owned-by schema", string(value.OwnedBy.Schema)); err != nil {
				return err
			}
			if err := validateIdentifier("owned-by table", string(value.OwnedBy.Table)); err != nil {
				return err
			}
			if err := validateIdentifier("owned-by column", string(value.OwnedBy.Column)); err != nil {
				return err
			}
		}
		return validateGrants(value.Grants)
	case schema.Table:
		if err := validateQualifiedObject(string(value.Schema), string(value.Name)); err != nil {
			return err
		}
		if err := validateOwner(value.Owner); err != nil {
			return err
		}
		if err := validateOptionalLiteral(value.Comment); err != nil {
			return err
		}
		for _, col := range value.Columns {
			if err := validateIdentifier("column", string(col.Name)); err != nil {
				return err
			}
			if col.Collation != nil {
				if _, err := sqlrender.ParseQualified(*col.Collation, 1, 2); err != nil {
					return fmt.Errorf("column %s collation: %w", col.Name, err)
				}
			}
			if err := validateOptionalLiteral(col.Comment); err != nil {
				return err
			}
		}
		if value.PrimaryKey != nil && value.PrimaryKey.Name != nil && *value.PrimaryKey.Name != "" {
			if err := validateIdentifier("primary key constraint", *value.PrimaryKey.Name); err != nil {
				return err
			}
		}
		if value.PrimaryKey != nil {
			if err := validateColumnNames(value.PrimaryKey.Cols); err != nil {
				return err
			}
		}
		for _, unique := range value.Uniques {
			if unique.NotValid {
				return fmt.Errorf("cannot render unique constraint %q as NOT VALID: PostgreSQL only permits NOT VALID for CHECK and FOREIGN KEY constraints", unique.Name)
			}
			if unique.Name != "" {
				if err := validateIdentifier("unique constraint", unique.Name); err != nil {
					return err
				}
			}
			if err := validateColumnNames(unique.Cols); err != nil {
				return err
			}
		}
		for _, check := range value.Checks {
			if check.Name != "" {
				if err := validateIdentifier("check constraint", check.Name); err != nil {
					return err
				}
			}
		}
		for _, foreignKey := range value.ForeignKeys {
			if err := validateIdentifier("foreign key constraint", foreignKey.Name); err != nil {
				return err
			}
			if err := validateColumnNames(foreignKey.Cols); err != nil {
				return err
			}
			if err := validateQualifiedObject(string(foreignKey.Ref.Schema), string(foreignKey.Ref.Table)); err != nil {
				return err
			}
			if err := validateColumnNames(foreignKey.Ref.Cols); err != nil {
				return err
			}
			if !validReferentialAction(foreignKey.OnDelete) || !validReferentialAction(foreignKey.OnUpdate) {
				return fmt.Errorf("cannot render foreign key %q with unknown referential action", foreignKey.Name)
			}
			if !validMatchType(foreignKey.Match) {
				return fmt.Errorf("cannot render foreign key %q with unknown match type %q", foreignKey.Name, foreignKey.Match)
			}
		}
		for _, inherited := range value.Inherits {
			if err := validateQualifiedObject(string(inherited.Schema), inherited.Name); err != nil {
				return err
			}
		}
		if value.Partition != nil {
			return fmt.Errorf("cannot render partitioned table %s.%s: partition clauses are not implemented", value.Schema, value.Name)
		}
		if len(value.Inherits) > 0 {
			return fmt.Errorf("cannot render inherited table %s.%s: INHERITS clauses are not implemented", value.Schema, value.Name)
		}
		return validateGrants(value.Grants)
	case schema.Index:
		if err := validateIdentifier("schema", string(value.Schema)); err != nil {
			return err
		}
		if err := validateIdentifier("table", string(value.Table)); err != nil {
			return err
		}
		if err := validateIdentifier("index", value.Name); err != nil {
			return err
		}
		if err := validateIdentifier("index method", string(value.Method)); err != nil {
			return err
		}
		for _, key := range value.KeyExprs {
			if key.Collation != nil {
				if _, err := sqlrender.ParseQualified(*key.Collation, 1, 2); err != nil {
					return fmt.Errorf("index %s collation: %w", value.Name, err)
				}
			}
			if key.OpClass != nil {
				if _, err := sqlrender.ParseQualified(*key.OpClass, 1, 2); err != nil {
					return fmt.Errorf("index %s operator class: %w", value.Name, err)
				}
			}
		}
		for _, col := range value.Include {
			if err := validateIdentifier("included column", string(col)); err != nil {
				return err
			}
		}
		return validateOptionalLiteral(value.Comment)
	case schema.View:
		if err := validateQualifiedObject(string(value.Schema), value.Name); err != nil {
			return err
		}
		if err := validateOwner(value.Owner); err != nil {
			return err
		}
		if err := validateOptionalLiteral(value.Comment); err != nil {
			return err
		}
		if value.Type != "" && value.Type != schema.RegularView && value.Type != schema.MaterializedView {
			return fmt.Errorf("cannot render view %s with unknown type %q", value.Name, value.Type)
		}
		for _, output := range value.Definition.OutputColumns {
			if err := validateIdentifier("view output column", output.Name); err != nil {
				return err
			}
		}
		return validateGrants(value.Grants)
	case schema.Function:
		if err := validateQualifiedObject(string(value.Schema), value.Name); err != nil {
			return err
		}
		if err := validateOwner(value.Owner); err != nil {
			return err
		}
		for _, arg := range value.Args {
			if arg.Name != nil {
				if err := validateIdentifier("function argument", *arg.Name); err != nil {
					return err
				}
			}
			if !validArgMode(arg.Mode) {
				return fmt.Errorf("cannot render function %s with unknown argument mode %q", value.Name, arg.Mode)
			}
		}
		switch returns := value.Returns.(type) {
		case schema.ReturnsType, schema.ReturnsSetOf:
		case schema.ReturnsTable:
			for _, col := range returns.Columns {
				if err := validateIdentifier("function result column", col.Name); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("cannot render function %s with return type %T", value.Name, value.Returns)
		}
		if err := validateIdentifier("function language", string(value.Language)); err != nil {
			return err
		}
		if !validVolatility(value.Volatility) || !validParallelSafety(value.Parallel) {
			return fmt.Errorf("cannot render function %s with unknown behavior keywords", value.Name)
		}
		for _, path := range value.SearchPath {
			if err := validateIdentifier("search_path schema", string(path)); err != nil {
				return err
			}
		}
		if err := validateOptionalLiteral(value.Comment); err != nil {
			return err
		}
		return validateGrants(value.Grants)
	case schema.Trigger:
		if err := validateQualifiedObject(string(value.Schema), string(value.Table)); err != nil {
			return err
		}
		if err := validateIdentifier("trigger", value.Name); err != nil {
			return err
		}
		if err := validateQualifiedObject(string(value.Function.Schema), value.Function.Name); err != nil {
			return err
		}
		if !validTriggerTiming(value.Timing) {
			return fmt.Errorf("cannot render trigger %s with unknown timing %q", value.Name, value.Timing)
		}
		for _, event := range value.Events {
			if !validTriggerEvent(event) {
				return fmt.Errorf("cannot render trigger %s with unknown event %q", value.Name, event)
			}
		}
		if value.When != nil {
			return fmt.Errorf("cannot render trigger %s with a WHEN clause: trigger WHEN rendering is not implemented", value.Name)
		}
		if value.Enabled != "" {
			return fmt.Errorf("cannot render trigger %s with enabled state %q: trigger state rendering is not implemented", value.Name, value.Enabled)
		}
		return nil
	case schema.Policy:
		if err := validateQualifiedObject(string(value.Schema), string(value.Table)); err != nil {
			return err
		}
		if err := validateIdentifier("policy", value.Name); err != nil {
			return err
		}
		if !validPolicyFor(value.For) {
			return fmt.Errorf("cannot render policy %s with unknown command %q", value.Name, value.For)
		}
		for _, role := range value.To {
			if _, err := sqlrender.Role(role); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("cannot validate unsupported object type %T", obj)
	}
}

func validateQualifiedObject(schemaName, objectName string) error {
	if err := validateIdentifier("schema", schemaName); err != nil {
		return err
	}
	return validateIdentifier("object", objectName)
}

func validateIdentifier(kind, value string) error {
	if err := sqlrender.ValidateIdentifier(value); err != nil {
		return fmt.Errorf("invalid %s name: %w", kind, err)
	}
	return nil
}

func validateColumnNames(values []schema.ColumnName) error {
	for _, value := range values {
		if err := validateIdentifier("column", string(value)); err != nil {
			return err
		}
	}
	return nil
}

func validateOptionalLiteral(value *string) error {
	if value == nil {
		return nil
	}
	_, err := sqlrender.Literal(*value)
	return err
}

func validPrivilege(value schema.Privilege) bool {
	switch value {
	case schema.PrivSelect, schema.PrivInsert, schema.PrivUpdate, schema.PrivDelete,
		schema.PrivTruncate, schema.PrivReferences, schema.PrivTrigger,
		schema.PrivExecute, schema.PrivUsage, schema.PrivCreate,
		schema.PrivConnect, schema.PrivTemporary, schema.PrivAll:
		return true
	default:
		return false
	}
}

func validateGrantRenderInputs(grantee string, privileges []schema.Privilege) error {
	if _, err := sqlrender.Role(grantee); err != nil {
		return err
	}
	if len(privileges) == 0 {
		return fmt.Errorf("cannot render an empty privilege list for grantee %q", grantee)
	}
	for _, privilege := range privileges {
		if !validPrivilege(privilege) {
			return fmt.Errorf("cannot render unknown privilege %q", privilege)
		}
	}
	return nil
}

func validReferentialAction(value schema.ReferentialAction) bool {
	switch value {
	case schema.NoAction, schema.Restrict, schema.Cascade, schema.SetNull, schema.SetDefault:
		return true
	default:
		return false
	}
}

func validMatchType(value schema.MatchType) bool {
	switch value {
	case "", schema.MatchSimple, schema.MatchFull, schema.MatchPartial:
		return true
	default:
		return false
	}
}

func validArgMode(value schema.ArgMode) bool {
	switch value {
	case "", schema.InMode, schema.OutMode, schema.InOutMode, schema.VariadicMode:
		return true
	default:
		return false
	}
}

func validVolatility(value schema.Volatility) bool {
	switch value {
	case "", schema.Immutable, schema.Stable, schema.Volatile:
		return true
	default:
		return false
	}
}

func validParallelSafety(value schema.ParallelSafety) bool {
	switch value {
	case "", schema.ParallelSafe, schema.ParallelRestricted, schema.ParallelUnsafe:
		return true
	default:
		return false
	}
}

func validTriggerTiming(value schema.TriggerTiming) bool {
	switch value {
	case schema.Before, schema.After, schema.InsteadOf:
		return true
	default:
		return false
	}
}

func validTriggerEvent(value schema.TriggerEvent) bool {
	switch value {
	case schema.Insert, schema.Update, schema.Delete, schema.Truncate:
		return true
	default:
		return false
	}
}

func validPolicyFor(value schema.PolicyFor) bool {
	switch value {
	case schema.ForAll, schema.ForSelect, schema.ForInsert, schema.ForUpdate, schema.ForDelete:
		return true
	default:
		return false
	}
}

func quotedIdentifier(value string) string {
	return sqlrender.Identifier(value)
}

func qualifiedName(schemaName, objectName string) string {
	return sqlrender.Qualified(schemaName, objectName)
}

func qualifiedColumnName(schemaName, tableName, columnName string) string {
	return sqlrender.Qualified(schemaName, tableName, columnName)
}

func quotedColumnNames(values []schema.ColumnName) string {
	strings := make([]string, len(values))
	for i := range values {
		strings[i] = string(values[i])
	}
	return sqlrender.IdentifierList(strings)
}

func quotedStringIdentifiers(values []string) string {
	return sqlrender.IdentifierList(values)
}

func quotedRole(value string) string {
	rendered, _ := sqlrender.Role(value) // validated at the public renderer boundary
	return rendered
}

func quotedLiteral(value string) string {
	rendered, _ := sqlrender.Literal(value) // validated at the public renderer boundary
	return rendered
}

func qualifiedText(value string) string {
	rendered, _ := sqlrender.ParseQualified(value, 1, 2) // validated at the public renderer boundary
	return rendered
}

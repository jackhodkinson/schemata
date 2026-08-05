package parser

import (
	"fmt"
	"strings"

	"github.com/jackhodkinson/schemata/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

// parseCreateView parses a CREATE VIEW statement
func (p *Parser) parseCreateView(stmt *pg_query.ViewStmt) (schema.DatabaseObject, error) {
	if stmt.View == nil {
		return nil, fmt.Errorf("CREATE VIEW missing view name")
	}
	if stmt.View.Relpersistence != "" && stmt.View.Relpersistence != "p" {
		return nil, fmt.Errorf("temporary views are not modeled")
	}
	if len(stmt.Aliases) > 0 || len(stmt.Options) > 0 || (stmt.WithCheckOption != pg_query.ViewCheckOption_NO_CHECK_OPTION && stmt.WithCheckOption != pg_query.ViewCheckOption_VIEW_CHECK_OPTION_UNDEFINED) {
		return nil, fmt.Errorf("CREATE VIEW uses output aliases, view options, or CHECK OPTION metadata that the declarative view model cannot preserve")
	}

	schemaName, viewName := p.extractQualifiedName(stmt.View)

	// Note: Materialized views are handled separately
	// ViewStmt is only for regular views
	viewType := schema.RegularView

	// Deparse the query - it's already a complete SELECT statement
	queryStr := ""
	if stmt.Query != nil {
		// Query is already a SelectStmt, deparse it directly
		result, err := pg_query.Deparse(&pg_query.ParseResult{
			Stmts: []*pg_query.RawStmt{{Stmt: stmt.Query}},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to deparse view %s.%s query: %w", schemaName, viewName, err)
		}
		queryStr = strings.TrimSpace(result)
		queryStr = strings.TrimSuffix(queryStr, ";")
		if queryStr == "" {
			return nil, fmt.Errorf("view %s.%s query deparsed to empty SQL", schemaName, viewName)
		}
	}

	return schema.View{
		Schema: schemaName,
		Name:   viewName,
		Type:   viewType,
		Definition: schema.ViewDefinition{
			Query: queryStr,
		},
	}, nil
}

// parseCreateMaterializedView parses the subset of CREATE MATERIALIZED VIEW
// represented by schema.View. Storage options, explicit output-column lists,
// tablespaces, access methods, and WITH NO DATA are rejected rather than lost.
func (p *Parser) parseCreateMaterializedView(stmt *pg_query.CreateTableAsStmt) (schema.DatabaseObject, error) {
	if stmt == nil || stmt.Into == nil || stmt.Into.Rel == nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW missing view name")
	}
	if stmt.Into.Rel.Relpersistence != "" && stmt.Into.Rel.Relpersistence != "p" {
		return nil, fmt.Errorf("temporary or unlogged materialized views are not modeled")
	}
	if len(stmt.Into.ColNames) > 0 || len(stmt.Into.Options) > 0 || stmt.Into.AccessMethod != "" || stmt.Into.TableSpaceName != "" || stmt.Into.SkipData || stmt.Into.OnCommit != pg_query.OnCommitAction_ONCOMMIT_NOOP || stmt.Into.ViewQuery != nil || stmt.IsSelectInto {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW uses output columns, storage options, an access method, a tablespace, or WITH NO DATA that the declarative view model cannot preserve")
	}
	if stmt.Query == nil {
		return nil, fmt.Errorf("CREATE MATERIALIZED VIEW missing query")
	}

	schemaName, viewName := p.extractQualifiedName(stmt.Into.Rel)
	query, err := pg_query.Deparse(&pg_query.ParseResult{Stmts: []*pg_query.RawStmt{{Stmt: stmt.Query}}})
	if err != nil {
		return nil, fmt.Errorf("failed to deparse materialized view %s.%s query: %w", schemaName, viewName, err)
	}
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	if query == "" {
		return nil, fmt.Errorf("materialized view %s.%s query deparsed to empty SQL", schemaName, viewName)
	}
	return schema.View{
		Schema: schemaName,
		Name:   viewName,
		Type:   schema.MaterializedView,
		Definition: schema.ViewDefinition{
			Query: query,
		},
	}, nil
}

// parseCreateSequence parses a CREATE SEQUENCE statement
func (p *Parser) parseCreateSequence(stmt *pg_query.CreateSeqStmt) (schema.DatabaseObject, error) {
	if stmt.Sequence == nil {
		return nil, fmt.Errorf("CREATE SEQUENCE missing sequence name")
	}
	if stmt.Sequence.Relpersistence != "" && stmt.Sequence.Relpersistence != "p" {
		return nil, fmt.Errorf("temporary or unlogged sequences are not modeled")
	}

	schemaName, seqName := p.extractQualifiedName(stmt.Sequence)

	seq := schema.Sequence{
		Schema: schemaName,
		Name:   seqName,
		Type:   "bigint", // Default
		Cycle:  false,
	}

	// Parse sequence options
	for _, option := range stmt.Options {
		if option == nil {
			continue
		}
		if defElem, ok := option.Node.(*pg_query.Node_DefElem); ok {
			if err := p.parseSequenceOption(defElem.DefElem, &seq); err != nil {
				return nil, fmt.Errorf("failed to parse sequence %s.%s option: %w", schemaName, seqName, err)
			}
		} else {
			return nil, fmt.Errorf("sequence %s.%s contains an unsupported option", schemaName, seqName)
		}
	}

	return seq, nil
}

// parseSequenceOption parses a sequence option
func (p *Parser) parseSequenceOption(opt *pg_query.DefElem, seq *schema.Sequence) error {
	if opt == nil || opt.Defname == "" {
		return fmt.Errorf("empty sequence option")
	}

	switch strings.ToLower(opt.Defname) {
	case "start":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.Start = val
			return nil
		}
	case "increment":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.Increment = val
			return nil
		}
	case "minvalue":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.MinValue = val
			return nil
		}
	case "maxvalue":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.MaxValue = val
			return nil
		}
	case "cache":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.Cache = val
			return nil
		}
	case "cycle":
		value := p.extractBoolValue(opt.Arg)
		seq.Cycle = value == nil || *value
		return nil
	case "as":
		if opt.Arg != nil && opt.Arg.GetTypeName() != nil {
			typeName, err := p.parseTypeName(opt.Arg.GetTypeName())
			if err != nil {
				return err
			}
			seq.Type = string(typeName)
			return nil
		}
	case "owned_by":
		names := extractStringList(opt.Arg)
		if len(names) == 1 && strings.EqualFold(names[0], "none") {
			return fmt.Errorf("OWNED BY NONE cannot be represented as an explicit desired removal")
		}
		if len(names) != 2 && len(names) != 3 {
			return fmt.Errorf("OWNED BY must name table.column or schema.table.column")
		}
		ownerSchema := seq.Schema
		if len(names) == 3 {
			ownerSchema = schema.SchemaName(names[0])
		}
		seq.OwnedBy = &schema.SequenceOwner{
			Schema: ownerSchema,
			Table:  schema.TableName(names[len(names)-2]),
			Column: schema.ColumnName(names[len(names)-1]),
		}
		return nil
	default:
		return fmt.Errorf("unsupported sequence option %q", opt.Defname)
	}
	return fmt.Errorf("sequence option %q is missing or has an unsupported value", opt.Defname)
}

// parseCreateFunction parses a CREATE FUNCTION statement
func (p *Parser) parseCreateFunction(stmt *pg_query.CreateFunctionStmt) (schema.DatabaseObject, error) {
	if stmt.IsProcedure {
		return nil, fmt.Errorf("CREATE PROCEDURE is not modeled as a function")
	}
	if len(stmt.Funcname) == 0 {
		return nil, fmt.Errorf("CREATE FUNCTION missing function name")
	}
	if len(stmt.Funcname) > 2 {
		return nil, fmt.Errorf("CREATE FUNCTION has an unsupported catalog-qualified name")
	}
	if stmt.SqlBody != nil {
		return nil, fmt.Errorf("SQL-standard function bodies are not modeled; use LANGUAGE ... AS with a string body")
	}

	// Extract schema and function name
	schemaName := schema.SchemaName("public")
	funcName := ""

	for i, node := range stmt.Funcname {
		strNode, ok := node.Node.(*pg_query.Node_String_)
		if !ok || strNode.String_ == nil || strNode.String_.Sval == "" {
			return nil, fmt.Errorf("CREATE FUNCTION contains an invalid name component")
		}
		if i == len(stmt.Funcname)-1 {
			funcName = strNode.String_.Sval
		} else {
			schemaName = schema.SchemaName(strNode.String_.Sval)
		}
	}

	function := schema.Function{
		Schema:     schemaName,
		Name:       funcName,
		Args:       []schema.FunctionArg{},
		Language:   schema.SQL,
		Volatility: schema.Volatile,
		Parallel:   schema.ParallelUnsafe,
	}

	// Parse parameters
	for _, param := range stmt.Parameters {
		if param == nil {
			continue
		}
		if funcParam, ok := param.Node.(*pg_query.Node_FunctionParameter); ok {
			arg, err := p.parseFunctionParameter(funcParam.FunctionParameter)
			if err != nil {
				return nil, fmt.Errorf("failed to parse function %s argument: %w", funcName, err)
			}
			function.Args = append(function.Args, arg)
		}
	}

	// RETURNS TABLE is represented by TABLE-mode parameters in PostgreSQL's
	// AST. Preserve that shape instead of treating those output columns as
	// ordinary input parameters.
	if hasTableFunctionArguments(function.Args) {
		function.Returns = inferFunctionReturnFromArgs(function.Args)
	} else if stmt.ReturnType != nil {
		if err := rejectFunctionTypeModifiers(stmt.ReturnType, "return type"); err != nil {
			return nil, fmt.Errorf("failed to parse function %s return type: %w", funcName, err)
		}
		returnType, err := p.parseTypeName(stmt.ReturnType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse function %s return type: %w", funcName, err)
		}
		if stmt.ReturnType.Setof {
			function.Returns = schema.ReturnsSetOf{Type: returnType}
		} else {
			function.Returns = schema.ReturnsType{Type: returnType}
		}
	}

	// Parse function options
	for _, option := range stmt.Options {
		if option == nil {
			continue
		}
		defElem, ok := option.Node.(*pg_query.Node_DefElem)
		if !ok || defElem.DefElem == nil {
			return nil, fmt.Errorf("function %s has an unsupported option node %T", funcName, option.Node)
		}
		if err := p.parseFunctionOption(defElem.DefElem, &function); err != nil {
			return nil, fmt.Errorf("failed to parse function %s option %q: %w", funcName, defElem.DefElem.Defname, err)
		}
	}

	// Functions with OUT/TABLE arguments can omit an explicit RETURNS clause.
	// Infer the effective return shape so parser and catalog extraction align.
	if function.Returns == nil {
		function.Returns = inferFunctionReturnFromArgs(function.Args)
	}

	return function, nil
}

func hasTableFunctionArguments(args []schema.FunctionArg) bool {
	for _, arg := range args {
		if arg.Mode == schema.TableMode {
			return true
		}
	}
	return false
}

func inferFunctionReturnFromArgs(args []schema.FunctionArg) schema.FunctionReturn {
	tableCols := make([]schema.TableColumn, 0)
	outArgs := make([]schema.FunctionArg, 0)
	for idx, arg := range args {
		switch arg.Mode {
		case schema.TableMode:
			colName := fmt.Sprintf("column_%d", idx+1)
			if arg.Name != nil && *arg.Name != "" {
				colName = *arg.Name
			}
			tableCols = append(tableCols, schema.TableColumn{Name: colName, Type: arg.Type})
		case schema.OutMode, schema.InOutMode:
			outArgs = append(outArgs, arg)
		}
	}

	if len(tableCols) > 0 {
		return schema.ReturnsTable{Columns: tableCols}
	}
	if len(outArgs) == 1 {
		return schema.ReturnsType{Type: outArgs[0].Type}
	}
	if len(outArgs) > 1 {
		return schema.ReturnsType{Type: "record"}
	}
	return nil
}

// parseFunctionParameter parses a function parameter
func (p *Parser) parseFunctionParameter(param *pg_query.FunctionParameter) (schema.FunctionArg, error) {
	if param == nil {
		return schema.FunctionArg{}, fmt.Errorf("empty function parameter")
	}
	arg := schema.FunctionArg{
		Mode: schema.InMode,
	}

	// Parameter name
	if param.Name != "" {
		name := param.Name
		arg.Name = &name
	}

	// Parameter type
	if param.ArgType != nil {
		if err := rejectFunctionTypeModifiers(param.ArgType, "argument type"); err != nil {
			return schema.FunctionArg{}, err
		}
		var err error
		arg.Type, err = p.parseTypeName(param.ArgType)
		if err != nil {
			return schema.FunctionArg{}, err
		}
	}

	// Parameter mode
	switch param.Mode {
	case pg_query.FunctionParameterMode_FUNC_PARAM_IN:
		arg.Mode = schema.InMode
	case pg_query.FunctionParameterMode_FUNC_PARAM_OUT:
		arg.Mode = schema.OutMode
	case pg_query.FunctionParameterMode_FUNC_PARAM_INOUT:
		arg.Mode = schema.InOutMode
	case pg_query.FunctionParameterMode_FUNC_PARAM_VARIADIC:
		arg.Mode = schema.VariadicMode
	case pg_query.FunctionParameterMode_FUNC_PARAM_TABLE:
		arg.Mode = schema.TableMode
	case pg_query.FunctionParameterMode_FUNCTION_PARAMETER_MODE_UNDEFINED,
		pg_query.FunctionParameterMode_FUNC_PARAM_DEFAULT:
		arg.Mode = schema.InMode
	default:
		return schema.FunctionArg{}, fmt.Errorf("unsupported function argument mode %q", param.Mode)
	}

	// Default value
	if param.Defexpr != nil {
		exprStr, err := p.deparseExpr(param.Defexpr)
		if err != nil {
			return schema.FunctionArg{}, err
		}
		expr := schema.Expr(exprStr)
		arg.Default = &expr
	}

	return arg, nil
}

// PostgreSQL discards type modifiers from function argument and return type
// identity (for example varchar(10) becomes varchar). Reject them in source
// schemas so parsing and catalog extraction cannot disagree about the object key.
func rejectFunctionTypeModifiers(typeName *pg_query.TypeName, context string) error {
	if typeName != nil && len(typeName.Typmods) > 0 {
		return fmt.Errorf("%s uses a type modifier that PostgreSQL does not preserve in function identity", context)
	}
	return nil
}

// parseFunctionOption parses function options (LANGUAGE, VOLATILITY, etc.)
// and fails closed when an option cannot be represented by schema.Function.
func (p *Parser) parseFunctionOption(opt *pg_query.DefElem, fn *schema.Function) error {
	if opt == nil || opt.Defname == "" {
		return fmt.Errorf("empty function option")
	}

	switch strings.ToLower(opt.Defname) {
	case "language":
		lang := p.extractStringValue(opt.Arg)
		if lang == "" {
			return fmt.Errorf("LANGUAGE is missing a value")
		}
		fn.Language = schema.Language(strings.ToLower(lang))
	case "volatility":
		switch strings.ToUpper(p.extractStringValue(opt.Arg)) {
		case "IMMUTABLE":
			fn.Volatility = schema.Immutable
		case "STABLE":
			fn.Volatility = schema.Stable
		case "VOLATILE":
			fn.Volatility = schema.Volatile
		default:
			return fmt.Errorf("VOLATILITY has an unknown value")
		}
	case "strict":
		strict := p.extractBoolValue(opt.Arg)
		if strict == nil {
			return fmt.Errorf("STRICT is missing a boolean value")
		}
		fn.Strict = *strict
	case "security":
		securityDefiner := p.extractBoolValue(opt.Arg)
		if securityDefiner == nil {
			return fmt.Errorf("SECURITY is missing a boolean value")
		}
		fn.SecurityDefiner = *securityDefiner
	case "parallel":
		switch strings.ToUpper(p.extractStringValue(opt.Arg)) {
		case "SAFE":
			fn.Parallel = schema.ParallelSafe
		case "RESTRICTED":
			fn.Parallel = schema.ParallelRestricted
		case "UNSAFE":
			fn.Parallel = schema.ParallelUnsafe
		default:
			return fmt.Errorf("PARALLEL has an unknown value")
		}
	case "set":
		if fn.SearchPath != nil {
			return fmt.Errorf("duplicate SET search_path clause")
		}
		path, err := parseFunctionSearchPathOption(opt.Arg)
		if err != nil {
			return err
		}
		fn.SearchPath = path
	case "as":
		body, err := parseSingleFunctionBody(opt.Arg)
		if err != nil {
			return err
		}
		fn.Body = body
	default:
		return fmt.Errorf("function option %q is not modeled", opt.Defname)
	}
	return nil
}

func parseSingleFunctionBody(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("AS is missing a function body")
	}
	switch value := node.Node.(type) {
	case *pg_query.Node_String_:
		if value.String_ == nil {
			return "", fmt.Errorf("AS contains an empty string node")
		}
		return value.String_.Sval, nil
	case *pg_query.Node_List:
		if value.List == nil || len(value.List.Items) != 1 {
			return "", fmt.Errorf("multi-part AS bodies used by C/internal functions are not modeled")
		}
		item, ok := value.List.Items[0].Node.(*pg_query.Node_String_)
		if !ok || item.String_ == nil {
			return "", fmt.Errorf("AS body has an unsupported AST node")
		}
		return item.String_.Sval, nil
	default:
		return "", fmt.Errorf("AS body has an unsupported AST node %T", node.Node)
	}
}

func parseFunctionSearchPathOption(node *pg_query.Node) ([]schema.SchemaName, error) {
	setNode, ok := node.Node.(*pg_query.Node_VariableSetStmt)
	if !ok || setNode.VariableSetStmt == nil {
		return nil, fmt.Errorf("SET option has unsupported AST node %T", node.Node)
	}
	set := setNode.VariableSetStmt
	if set.Name != "search_path" {
		return nil, fmt.Errorf("function configuration key %q is not modeled", set.Name)
	}
	if set.Kind != pg_query.VariableSetKind_VAR_SET_VALUE || set.IsLocal {
		return nil, fmt.Errorf("SET search_path must use an explicit value list")
	}
	if len(set.Args) == 0 {
		return nil, fmt.Errorf("SET search_path has no values")
	}

	path := make([]schema.SchemaName, 0, len(set.Args))
	for i, arg := range set.Args {
		value, ok := functionOptionStringValue(arg)
		if !ok {
			return nil, fmt.Errorf("SET search_path item %d is not a string", i+1)
		}
		if value == "" {
			if len(set.Args) != 1 {
				return nil, fmt.Errorf("empty search_path cannot be combined with schema names")
			}
			return make([]schema.SchemaName, 0), nil
		}
		path = append(path, schema.SchemaName(value))
	}
	return path, nil
}

func functionOptionStringValue(node *pg_query.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	switch value := node.Node.(type) {
	case *pg_query.Node_String_:
		return value.String_.Sval, true
	case *pg_query.Node_AConst:
		if stringValue := value.AConst.GetSval(); stringValue != nil {
			return stringValue.Sval, true
		}
	}
	return "", false
}

// parseCreateTrigger parses a CREATE TRIGGER statement
func (p *Parser) parseCreateTrigger(stmt *pg_query.CreateTrigStmt) (schema.DatabaseObject, error) {
	if stmt.Relation == nil {
		return nil, fmt.Errorf("CREATE TRIGGER missing relation")
	}

	schemaName, tableName := p.extractQualifiedName(stmt.Relation)

	trigger := schema.Trigger{
		Schema:     schemaName,
		Table:      schema.TableName(tableName),
		Name:       stmt.Trigname,
		ForEachRow: stmt.Row,
		Events:     []schema.TriggerEvent{},
	}

	// Parse timing (bitfield: 2=BEFORE, 64=INSTEAD OF, 0=AFTER)
	// Note: AFTER is the default in PostgreSQL (TRIGGER_TYPE_AFTER = 0),
	// so it's the absence of BEFORE and INSTEAD OF bits.
	timing := stmt.Timing
	if timing&2 != 0 {
		trigger.Timing = schema.Before
	} else if timing&64 != 0 {
		trigger.Timing = schema.InsteadOf
	} else {
		trigger.Timing = schema.After
	}

	// Parse events (bitfield: 4=INSERT, 8=DELETE, 16=UPDATE, 32=TRUNCATE)
	events := stmt.Events
	if events&4 != 0 {
		trigger.Events = append(trigger.Events, schema.Insert)
	}
	if events&8 != 0 {
		trigger.Events = append(trigger.Events, schema.Delete)
	}
	if events&16 != 0 {
		trigger.Events = append(trigger.Events, schema.Update)
	}
	if events&32 != 0 {
		trigger.Events = append(trigger.Events, schema.Truncate)
	}

	// Parse function name
	if len(stmt.Funcname) > 0 {
		funcSchema := schema.SchemaName("public")
		funcName := ""
		for i, node := range stmt.Funcname {
			if strNode, ok := node.Node.(*pg_query.Node_String_); ok {
				if i == len(stmt.Funcname)-1 {
					funcName = strNode.String_.Sval
				} else {
					funcSchema = schema.SchemaName(strNode.String_.Sval)
				}
			}
		}
		trigger.Function = schema.QualifiedName{
			Schema: funcSchema,
			Name:   funcName,
		}
	}

	return trigger, nil
}

// parseCreatePolicy parses a CREATE POLICY statement
func (p *Parser) parseCreatePolicy(stmt *pg_query.CreatePolicyStmt) (schema.DatabaseObject, error) {
	if stmt.Table == nil {
		return nil, fmt.Errorf("CREATE POLICY missing table")
	}

	schemaName, tableName := p.extractQualifiedName(stmt.Table)

	policy := schema.Policy{
		Schema:     schemaName,
		Table:      schema.TableName(tableName),
		Name:       stmt.PolicyName,
		Permissive: stmt.Permissive,
		For:        schema.ForAll,
		To:         []string{},
	}

	// Parse command (FOR clause)
	switch stmt.CmdName {
	case "select":
		policy.For = schema.ForSelect
	case "insert":
		policy.For = schema.ForInsert
	case "update":
		policy.For = schema.ForUpdate
	case "delete":
		policy.For = schema.ForDelete
	default:
		policy.For = schema.ForAll
	}

	// Parse roles (TO clause)
	for _, role := range stmt.Roles {
		if role == nil {
			continue
		}
		if roleSpec, ok := role.Node.(*pg_query.Node_RoleSpec); ok {
			if roleSpec.RoleSpec.Rolename != "" {
				policy.To = append(policy.To, roleSpec.RoleSpec.Rolename)
			}
		}
	}

	// Parse USING clause
	if stmt.Qual != nil {
		usingStr, err := p.deparseExpr(stmt.Qual)
		if err != nil {
			return nil, fmt.Errorf("failed to parse policy %s USING expression: %w", policy.Name, err)
		}
		using := schema.Expr(usingStr)
		policy.Using = &using
	}

	// Parse WITH CHECK clause
	if stmt.WithCheck != nil {
		withCheckStr, err := p.deparseExpr(stmt.WithCheck)
		if err != nil {
			return nil, fmt.Errorf("failed to parse policy %s WITH CHECK expression: %w", policy.Name, err)
		}
		withCheck := schema.Expr(withCheckStr)
		policy.WithCheck = &withCheck
	}

	return policy, nil
}

// parseCreateExtension parses a CREATE EXTENSION statement
func (p *Parser) parseCreateExtension(stmt *pg_query.CreateExtensionStmt) (schema.DatabaseObject, error) {
	ext := schema.Extension{
		Schema: schema.SchemaName("public"), // Extensions go in public by default
		Name:   stmt.Extname,
	}

	return ext, nil
}

// parseCreateSchema parses a CREATE SCHEMA statement
func (p *Parser) parseCreateSchema(stmt *pg_query.CreateSchemaStmt) (schema.DatabaseObject, error) {
	return schema.Schema{
		Name: schema.SchemaName(stmt.Schemaname),
	}, nil
}

// Helper: extract integer value from a node
func (p *Parser) extractIntValue(node *pg_query.Node) *int64 {
	if node == nil {
		return nil
	}
	if intNode, ok := node.Node.(*pg_query.Node_Integer); ok {
		val := int64(intNode.Integer.Ival)
		return &val
	}
	return nil
}

// Helper: extract string value from a node
func (p *Parser) extractStringValue(node *pg_query.Node) string {
	if node == nil {
		return ""
	}
	if strNode, ok := node.Node.(*pg_query.Node_String_); ok {
		return strNode.String_.Sval
	}
	return ""
}

// Helper: extract boolean value from a node
func (p *Parser) extractBoolValue(node *pg_query.Node) *bool {
	if node == nil {
		return nil
	}
	if boolNode, ok := node.Node.(*pg_query.Node_Boolean); ok {
		val := boolNode.Boolean.Boolval
		return &val
	}
	return nil
}

// Helper: extract list of string values from a node (for function bodies, etc.)
func (p *Parser) extractListValues(node *pg_query.Node) []string {
	if node == nil {
		return nil
	}
	if listNode, ok := node.Node.(*pg_query.Node_List); ok {
		var values []string
		for _, item := range listNode.List.Items {
			if str := p.extractStringValue(item); str != "" {
				values = append(values, str)
			}
		}
		return values
	}
	return nil
}

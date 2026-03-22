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
		if err == nil {
			queryStr = strings.TrimSpace(result)
			queryStr = strings.TrimSuffix(queryStr, ";")
		} else {
			queryStr = "(view query)"
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

// parseCreateSequence parses a CREATE SEQUENCE statement
func (p *Parser) parseCreateSequence(stmt *pg_query.CreateSeqStmt) (schema.DatabaseObject, error) {
	if stmt.Sequence == nil {
		return nil, fmt.Errorf("CREATE SEQUENCE missing sequence name")
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
			p.parseSequenceOption(defElem.DefElem, &seq)
		}
	}

	return seq, nil
}

// parseSequenceOption parses a sequence option
func (p *Parser) parseSequenceOption(opt *pg_query.DefElem, seq *schema.Sequence) {
	if opt == nil || opt.Defname == "" {
		return
	}

	switch strings.ToLower(opt.Defname) {
	case "start":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.Start = val
		}
	case "increment":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.Increment = val
		}
	case "minvalue":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.MinValue = val
		}
	case "maxvalue":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.MaxValue = val
		}
	case "cache":
		if val := p.extractIntValue(opt.Arg); val != nil {
			seq.Cache = val
		}
	case "cycle":
		seq.Cycle = true
	case "as":
		// Type specification
		if typeName := p.extractStringValue(opt.Arg); typeName != "" {
			seq.Type = typeName
		}
	}
}

// parseCreateFunction parses a CREATE FUNCTION statement
func (p *Parser) parseCreateFunction(stmt *pg_query.CreateFunctionStmt) (schema.DatabaseObject, error) {
	if len(stmt.Funcname) == 0 {
		return nil, fmt.Errorf("CREATE FUNCTION missing function name")
	}

	// Extract schema and function name
	schemaName := schema.SchemaName("public")
	funcName := ""

	for i, node := range stmt.Funcname {
		if strNode, ok := node.Node.(*pg_query.Node_String_); ok {
			if i == len(stmt.Funcname)-1 {
				funcName = strNode.String_.Sval
			} else {
				schemaName = schema.SchemaName(strNode.String_.Sval)
			}
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
			arg := p.parseFunctionParameter(funcParam.FunctionParameter)
			function.Args = append(function.Args, arg)
		}
	}

	// Parse return type
	if stmt.ReturnType != nil {
		function.Returns = schema.ReturnsType{
			Type: p.parseTypeName(stmt.ReturnType),
		}
	}

	// Parse function options
	for _, option := range stmt.Options {
		if option == nil {
			continue
		}
		if defElem, ok := option.Node.(*pg_query.Node_DefElem); ok {
			p.parseFunctionOption(defElem.DefElem, &function)
		}
	}

	// Functions with OUT/TABLE arguments can omit an explicit RETURNS clause.
	// Infer the effective return shape so parser and catalog extraction align.
	if function.Returns == nil {
		function.Returns = inferFunctionReturnFromArgs(function.Args)
	}

	return function, nil
}

func inferFunctionReturnFromArgs(args []schema.FunctionArg) schema.FunctionReturn {
	outCols := make([]schema.TableColumn, 0)
	for idx, arg := range args {
		if arg.Mode != schema.OutMode && arg.Mode != schema.TableMode {
			continue
		}

		colName := fmt.Sprintf("column_%d", idx+1)
		if arg.Name != nil && *arg.Name != "" {
			colName = *arg.Name
		}

		outCols = append(outCols, schema.TableColumn{
			Name: colName,
			Type: arg.Type,
		})
	}

	switch len(outCols) {
	case 0:
		return nil
	case 1:
		return schema.ReturnsType{Type: outCols[0].Type}
	default:
		return schema.ReturnsTable{Columns: outCols}
	}
}

// parseFunctionParameter parses a function parameter
func (p *Parser) parseFunctionParameter(param *pg_query.FunctionParameter) schema.FunctionArg {
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
		arg.Type = p.parseTypeName(param.ArgType)
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
	}

	// Default value
	if param.Defexpr != nil {
		exprStr := p.deparseExpr(param.Defexpr)
		expr := schema.Expr(exprStr)
		arg.Default = &expr
	}

	return arg
}

// parseFunctionOption parses function options (LANGUAGE, VOLATILITY, etc.)
func (p *Parser) parseFunctionOption(opt *pg_query.DefElem, fn *schema.Function) {
	if opt == nil || opt.Defname == "" {
		return
	}

	switch strings.ToLower(opt.Defname) {
	case "language":
		if lang := p.extractStringValue(opt.Arg); lang != "" {
			fn.Language = schema.Language(strings.ToLower(lang))
		}
	case "volatility":
		if vol := p.extractStringValue(opt.Arg); vol != "" {
			switch strings.ToUpper(vol) {
			case "IMMUTABLE":
				fn.Volatility = schema.Immutable
			case "STABLE":
				fn.Volatility = schema.Stable
			case "VOLATILE":
				fn.Volatility = schema.Volatile
			}
		}
	case "strict":
		fn.Strict = true
	case "security":
		fn.SecurityDefiner = true
	case "as":
		// Function body - can be a single string or a list of strings
		if body := p.extractStringValue(opt.Arg); body != "" {
			fn.Body = body
		} else if bodyParts := p.extractListValues(opt.Arg); len(bodyParts) > 0 {
			// PL/pgSQL functions often have body as a list of strings
			fn.Body = strings.Join(bodyParts, "\n")
		}
	}
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
		usingStr := p.deparseExpr(stmt.Qual)
		using := schema.Expr(usingStr)
		policy.Using = &using
	}

	// Parse WITH CHECK clause
	if stmt.WithCheck != nil {
		withCheckStr := p.deparseExpr(stmt.WithCheck)
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

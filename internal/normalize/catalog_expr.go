package normalize

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

// ExprWithoutLiteralCasts canonicalizes a catalog-rendered expression while
// removing resolver-added casts in narrowly proven-safe AST shapes. PostgreSQL
// casts untyped literals during resolution and casts varchar COALESCE results
// to text when selecting the built-in concatenation operator. Restricting the
// rewrite to those shapes avoids treating cast-looking string contents or
// arbitrary casts on columns and other expressions as equivalent.
//
// This operation is intentionally asymmetric and should only be used for the
// catalog side of a desired-versus-actual comparison.
func ExprWithoutLiteralCasts(expr schema.Expr) schema.Expr {
	exprStr := strings.TrimSpace(string(expr))
	if exprStr == "" || strings.IndexByte(exprStr, 0) >= 0 {
		return Expr(schema.Expr(exprStr))
	}

	canonical, err := canonicalizeExprOnceWithTransforms(
		exprStr,
		stripPublicRegclassQualifications,
		stripResolvedConcatCoalesceCasts,
		stripLiteralTypeCasts,
	)
	if err != nil || canonical == "" {
		return Expr(schema.Expr(exprStr))
	}
	return Expr(schema.Expr(canonical))
}

// stripResolvedConcatCoalesceCasts removes only a ::text coercion around a
// COALESCE operand of || when that COALESCE contains a resolver-cast literal.
// pg_get_expr emits this shape for varchar concatenation, for example:
//
//	COALESCE(first_name, ''::varchar)::text || ' '::text
//
// The same cast outside concatenation, or a cast on an arbitrary expression,
// remains semantic and is preserved.
func stripResolvedConcatCoalesceCasts(message protoreflect.Message) {
	if node, ok := message.Interface().(*pg_query.Node); ok {
		if expression := node.GetAExpr(); expression != nil && isConcatOperator(expression.Name) {
			expression.Lexpr = unwrapResolvedConcatCoalesceCast(expression.Lexpr)
			expression.Rexpr = unwrapResolvedConcatCoalesceCast(expression.Rexpr)
		}
	}

	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Kind() == protoreflect.MessageKind:
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				stripResolvedConcatCoalesceCasts(list.Get(i).Message())
			}
		case field.IsMap() && field.MapValue().Kind() == protoreflect.MessageKind:
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				stripResolvedConcatCoalesceCasts(item.Message())
				return true
			})
		case !field.IsList() && !field.IsMap() && field.Kind() == protoreflect.MessageKind:
			stripResolvedConcatCoalesceCasts(value.Message())
		}
		return true
	})
}

func isConcatOperator(names []*pg_query.Node) bool {
	return len(names) == 1 && names[0].GetString_() != nil && names[0].GetString_().Sval == "||"
}

func unwrapResolvedConcatCoalesceCast(node *pg_query.Node) *pg_query.Node {
	if node == nil {
		return node
	}
	cast := node.GetTypeCast()
	if cast == nil || cast.Arg == nil || !isExactBuiltinType(cast.TypeName, "text") {
		return node
	}
	coalesce := cast.Arg.GetCoalesceExpr()
	if coalesce == nil {
		return node
	}
	for _, argument := range coalesce.Args {
		argumentCast := argument.GetTypeCast()
		if argumentCast != nil && argumentCast.Arg != nil && argumentCast.Arg.GetAConst() != nil {
			return cast.Arg
		}
	}
	return node
}

func isExactBuiltinType(typeName *pg_query.TypeName, wanted string) bool {
	if typeName == nil || len(typeName.Names) == 0 || len(typeName.Names) > 2 {
		return false
	}
	name := typeName.Names[len(typeName.Names)-1].GetString_()
	if name == nil || name.Sval != wanted {
		return false
	}
	if len(typeName.Names) == 1 {
		return true
	}
	qualifier := typeName.Names[0].GetString_()
	return qualifier != nil && qualifier.Sval == "pg_catalog"
}

// stripLiteralTypeCasts walks children first so a chain of casts is removed
// only while every successive operand has reduced to a literal.
func stripLiteralTypeCasts(message protoreflect.Message) {
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Kind() == protoreflect.MessageKind:
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				stripLiteralTypeCasts(list.Get(i).Message())
			}
		case field.IsMap() && field.MapValue().Kind() == protoreflect.MessageKind:
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				stripLiteralTypeCasts(item.Message())
				return true
			})
		case !field.IsList() && !field.IsMap() && field.Kind() == protoreflect.MessageKind:
			stripLiteralTypeCasts(value.Message())
		}
		return true
	})

	node, ok := message.Interface().(*pg_query.Node)
	if !ok {
		return
	}
	for {
		cast := node.GetTypeCast()
		if cast == nil || cast.Arg == nil || cast.Arg.GetAConst() == nil {
			return
		}
		node.Node = cast.Arg.GetNode()
	}
}

package schema

import "strings"

// FunctionSignature returns the canonical "(t1,t2,...)" signature for function identity.
// It uses normalized argument types only (names/defaults are excluded).
func FunctionSignature(args []FunctionArg) string {
	if len(args) == 0 {
		return "()"
	}
	types := make([]string, len(args))
	for i, arg := range args {
		types[i] = string(NormalizeTypeName(arg.Type))
	}
	return "(" + strings.Join(types, ",") + ")"
}


package schema

import "strings"

// FunctionSignature returns the canonical "(t1,t2,...)" signature for function identity.
// It uses normalized argument types only (names/defaults are excluded).
func FunctionSignature(args []FunctionArg) string {
	identityTypes := make([]string, 0, len(args))
	for _, arg := range args {
		// PostgreSQL function identity arguments exclude OUT/TABLE args.
		if arg.Mode == OutMode || arg.Mode == TableMode {
			continue
		}
		identityTypes = append(identityTypes, string(NormalizeTypeName(arg.Type)))
	}

	if len(identityTypes) == 0 {
		return "()"
	}
	return "(" + strings.Join(identityTypes, ",") + ")"
}


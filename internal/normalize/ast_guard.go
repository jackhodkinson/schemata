package normalize

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// libpg_query's deparser recursively walks the protobuf AST on the C
	// stack. Go's panic/recover cannot intercept the SIGBUS/SIGSEGV caused by
	// exhausting that stack, so reject pathological trees before crossing the
	// cgo boundary. These limits are deliberately generous for real schema SQL
	// while still placing hard bounds on depth, width, and retained text.
	maxDeparseASTDepth              = 256
	maxDeparseASTMessages           = 50_000
	maxDeparseASTCollectionElements = 100_000
	maxDeparseASTScalarBytes        = 8 << 20
)

type astResourceLimits struct {
	maxDepth              int
	maxMessages           int
	maxCollectionElements int
	maxScalarBytes        int
}

type pendingASTMessage struct {
	message protoreflect.Message
	depth   int
}

type astResourceUsage struct {
	messages           int
	collectionElements int
	scalarBytes        int
}

// validateASTForDeparse walks the protobuf iteratively. An iterative walk is
// important here: using a recursive Go visitor as the safety check would move
// the same denial-of-service risk to the guard itself.
func validateASTForDeparse(root protoreflect.Message) error {
	return validateASTResources(root, astResourceLimits{
		maxDepth:              maxDeparseASTDepth,
		maxMessages:           maxDeparseASTMessages,
		maxCollectionElements: maxDeparseASTCollectionElements,
		maxScalarBytes:        maxDeparseASTScalarBytes,
	})
}

func validateASTResources(root protoreflect.Message, limits astResourceLimits) error {
	if !root.IsValid() {
		return fmt.Errorf("refusing to deparse an invalid PostgreSQL AST")
	}
	if limits.maxDepth <= 0 || limits.maxMessages <= 0 || limits.maxCollectionElements <= 0 || limits.maxScalarBytes <= 0 {
		return fmt.Errorf("invalid PostgreSQL AST resource limits")
	}

	stack := []pendingASTMessage{{message: root, depth: 1}}
	usage := astResourceUsage{}
	for len(stack) > 0 {
		last := len(stack) - 1
		pending := stack[last]
		stack = stack[:last]

		if pending.depth > limits.maxDepth {
			return fmt.Errorf(
				"refusing to deparse PostgreSQL AST: depth exceeds safe limit of %d",
				limits.maxDepth,
			)
		}
		if err := consumeASTResource(&usage.messages, 1, limits.maxMessages, "message count"); err != nil {
			return err
		}

		var walkErr error
		pending.message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			walkErr = visitASTField(&stack, pending.depth+1, field, value, &usage, limits)
			return walkErr == nil
		})
		if walkErr != nil {
			return walkErr
		}
	}

	return nil
}

func visitASTField(
	stack *[]pendingASTMessage,
	depth int,
	field protoreflect.FieldDescriptor,
	value protoreflect.Value,
	usage *astResourceUsage,
	limits astResourceLimits,
) error {
	switch {
	case field.IsMap():
		items := value.Map()
		if err := consumeASTResource(
			&usage.collectionElements,
			items.Len(),
			limits.maxCollectionElements,
			"collection element count",
		); err != nil {
			return err
		}
		var visitErr error
		items.Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
			if visitErr = visitASTValue(stack, depth, field.MapKey().Kind(), key.Value(), usage, limits); visitErr != nil {
				return false
			}
			visitErr = visitASTValue(stack, depth, field.MapValue().Kind(), item, usage, limits)
			return visitErr == nil
		})
		return visitErr

	case field.IsList():
		items := value.List()
		if err := consumeASTResource(
			&usage.collectionElements,
			items.Len(),
			limits.maxCollectionElements,
			"collection element count",
		); err != nil {
			return err
		}
		for i := 0; i < items.Len(); i++ {
			if err := visitASTValue(stack, depth, field.Kind(), items.Get(i), usage, limits); err != nil {
				return err
			}
		}
		return nil

	default:
		return visitASTValue(stack, depth, field.Kind(), value, usage, limits)
	}
}

func visitASTValue(
	stack *[]pendingASTMessage,
	depth int,
	kind protoreflect.Kind,
	value protoreflect.Value,
	usage *astResourceUsage,
	limits astResourceLimits,
) error {
	if kind == protoreflect.MessageKind || kind == protoreflect.GroupKind {
		*stack = append(*stack, pendingASTMessage{message: value.Message(), depth: depth})
		return nil
	}

	var scalarBytes int
	switch kind {
	case protoreflect.StringKind:
		scalarBytes = len(value.String())
	case protoreflect.BytesKind:
		scalarBytes = len(value.Bytes())
	default:
		return nil
	}
	return consumeASTResource(&usage.scalarBytes, scalarBytes, limits.maxScalarBytes, "scalar byte count")
}

func consumeASTResource(current *int, amount, limit int, resource string) error {
	if amount > limit-*current {
		return fmt.Errorf("refusing to deparse PostgreSQL AST: %s exceeds safe limit of %d", resource, limit)
	}
	*current += amount
	return nil
}

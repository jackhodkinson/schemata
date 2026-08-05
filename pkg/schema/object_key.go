package schema

// ObjectKeyFor returns the canonical database identity for an object. Keeping
// identity construction with the model prevents parser, catalog, differ, and
// planner layers from inventing subtly different keys.
func ObjectKeyFor(object DatabaseObject) ObjectKey {
	switch value := object.(type) {
	case Table:
		return ObjectKey{Kind: TableKind, Schema: value.Schema, Name: string(value.Name)}
	case Index:
		return ObjectKey{Kind: IndexKind, Schema: value.Schema, Name: value.Name, TableName: value.Table}
	case View:
		return ObjectKey{Kind: ViewKind, Schema: value.Schema, Name: value.Name}
	case Function:
		return ObjectKey{Kind: FunctionKind, Schema: value.Schema, Name: value.Name, Signature: FunctionSignature(value.Args)}
	case Sequence:
		return ObjectKey{Kind: SequenceKind, Schema: value.Schema, Name: value.Name}
	case EnumDef:
		return ObjectKey{Kind: TypeKind, Schema: value.Schema, Name: string(value.Name)}
	case DomainDef:
		return ObjectKey{Kind: TypeKind, Schema: value.Schema, Name: string(value.Name)}
	case CompositeDef:
		return ObjectKey{Kind: TypeKind, Schema: value.Schema, Name: string(value.Name)}
	case Trigger:
		return ObjectKey{Kind: TriggerKind, Schema: value.Schema, Name: value.Name, TableName: value.Table}
	case Policy:
		return ObjectKey{Kind: PolicyKind, Schema: value.Schema, Name: value.Name, TableName: value.Table}
	case Extension:
		return ObjectKey{Kind: ExtensionKind, Schema: value.Schema, Name: value.Name}
	case Schema:
		return ObjectKey{Kind: SchemaKind, Schema: value.Name, Name: string(value.Name)}
	default:
		return ObjectKey{}
	}
}

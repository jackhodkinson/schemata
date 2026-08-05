// Package capability is the central registry for Schemata's PostgreSQL object
// support. It makes partial and unsupported pipeline stages explicit instead
// of allowing a parser, planner, or renderer to silently omit an object.
package capability

import (
	"fmt"

	"github.com/jackhodkinson/schemata/pkg/schema"
)

type Family string

const (
	SchemaFamily           Family = "schema"
	ExtensionFamily        Family = "extension"
	EnumFamily             Family = "enum"
	DomainFamily           Family = "domain"
	CompositeFamily        Family = "composite"
	SequenceFamily         Family = "sequence"
	TableFamily            Family = "table"
	ColumnFamily           Family = "column"
	PrimaryKeyFamily       Family = "primary_key"
	UniqueConstraintFamily Family = "unique_constraint"
	CheckConstraintFamily  Family = "check_constraint"
	ForeignKeyFamily       Family = "foreign_key"
	IndexFamily            Family = "index"
	ViewFamily             Family = "view"
	MaterializedViewFamily Family = "materialized_view"
	FunctionFamily         Family = "function"
	TriggerFamily          Family = "trigger"
	GrantOwnershipFamily   Family = "grant_ownership"
	PolicyFamily           Family = "policy"
)

// V1Families is the object-family scope advertised by the production plan.
// PolicyFamily is intentionally separate because policies remain optional for
// v1, but it is still registered in Matrix so its behavior cannot be implicit.
var V1Families = []Family{
	SchemaFamily,
	ExtensionFamily,
	EnumFamily,
	DomainFamily,
	CompositeFamily,
	SequenceFamily,
	TableFamily,
	ColumnFamily,
	PrimaryKeyFamily,
	UniqueConstraintFamily,
	CheckConstraintFamily,
	ForeignKeyFamily,
	IndexFamily,
	ViewFamily,
	MaterializedViewFamily,
	FunctionFamily,
	TriggerFamily,
	GrantOwnershipFamily,
}

type Stage string

const (
	CaptureStage   Stage = "capture"
	ParseStage     Stage = "parse"
	NormalizeStage Stage = "normalize"
	CompareStage   Stage = "compare"
	CreateStage    Stage = "plan_create"
	AlterStage     Stage = "plan_alter"
	DropStage      Stage = "plan_drop"
)

var Stages = []Stage{
	CaptureStage,
	ParseStage,
	NormalizeStage,
	CompareStage,
	CreateStage,
	AlterStage,
	DropStage,
}

type Level string

const (
	Supported   Level = "supported"
	Partial     Level = "partial"
	Unsupported Level = "unsupported"
)

type Status struct {
	Level Level
	Note  string
}

// Pipeline has named fields so adding a new stage causes the registry and its
// validation test to change conspicuously rather than extending an untyped map.
type Pipeline struct {
	Capture   Status
	Parse     Status
	Normalize Status
	Compare   Status
	Create    Status
	Alter     Status
	Drop      Status
}

func (p Pipeline) At(stage Stage) Status {
	switch stage {
	case CaptureStage:
		return p.Capture
	case ParseStage:
		return p.Parse
	case NormalizeStage:
		return p.Normalize
	case CompareStage:
		return p.Compare
	case CreateStage:
		return p.Create
	case AlterStage:
		return p.Alter
	case DropStage:
		return p.Drop
	default:
		return Status{}
	}
}

type Entry struct {
	Family   Family
	Required bool
	Pipeline Pipeline
}

func yes() Status { return Status{Level: Supported} }
func no(note string) Status {
	return Status{Level: Unsupported, Note: note}
}
func partial(note string) Status { return Status{Level: Partial, Note: note} }

// Matrix is descriptive current-state data, not an aspirational claim. A
// Partial or Unsupported cell is a release blocker for required v1 families.
var Matrix = []Entry{
	{SchemaFamily, true, Pipeline{no("catalog extraction is not implemented"), yes(), yes(), yes(), no("CREATE SCHEMA rendering is not implemented"), no("schema alteration is not implemented"), no("DROP SCHEMA planning is not implemented")}},
	{ExtensionFamily, true, Pipeline{yes(), yes(), yes(), yes(), yes(), no("extension version/schema alteration is not implemented"), no("DROP EXTENSION planning is not implemented")}},
	{EnumFamily, true, Pipeline{yes(), yes(), yes(), yes(), yes(), partial("append-only values and comments"), yes()}},
	{DomainFamily, true, Pipeline{yes(), yes(), yes(), yes(), yes(), partial("implemented as drop/recreate and therefore dependency-sensitive"), yes()}},
	{CompositeFamily, true, Pipeline{no("catalog extraction is not implemented"), yes(), yes(), yes(), no("CREATE TYPE AS composite rendering is not implemented"), no("composite alteration is not implemented"), yes()}},
	{SequenceFamily, true, Pipeline{yes(), yes(), yes(), yes(), partial("ownership, OWNED BY, type, comments, and grants are not all emitted on create"), partial("non-ACL changes use drop/recreate"), yes()}},
	{TableFamily, true, Pipeline{partial("partition and inheritance catalog metadata is incomplete"), partial("partition and inheritance variants are incomplete"), yes(), yes(), partial("partition and inheritance clauses are not emitted"), partial("only explicitly modeled table changes are rendered"), yes()}},
	{ColumnFamily, true, Pipeline{yes(), yes(), yes(), yes(), yes(), partial("some changes require explicit migrations"), yes()}},
	{PrimaryKeyFamily, true, Pipeline{yes(), yes(), yes(), yes(), yes(), yes(), yes()}},
	{UniqueConstraintFamily, true, Pipeline{partial("NULLS NOT DISTINCT extraction is incomplete"), yes(), yes(), yes(), yes(), partial("some property changes require constraint recreation"), yes()}},
	{CheckConstraintFamily, true, Pipeline{yes(), yes(), yes(), yes(), yes(), yes(), yes()}},
	{ForeignKeyFamily, true, Pipeline{partial("multi-column extraction requires an ordinal-safe catalog query"), yes(), yes(), yes(), yes(), yes(), yes()}},
	{IndexFamily, true, Pipeline{yes(), yes(), yes(), yes(), partial("operator-class qualification and raw expression validation remain incomplete"), partial("implemented through drop/create"), yes()}},
	{ViewFamily, true, Pipeline{yes(), yes(), yes(), yes(), partial("options, comments, owner, and grants are not all emitted on create"), partial("definition changes use drop/create"), yes()}},
	{MaterializedViewFamily, true, Pipeline{yes(), yes(), yes(), yes(), partial("materialized-view options and population state are not modeled"), partial("definition changes use drop/create"), partial("DROP currently uses VIEW rather than MATERIALIZED VIEW")}},
	{FunctionFamily, true, Pipeline{partial("configuration and complete function attributes are not all captured"), partial("only functions, not procedures, are modeled"), yes(), yes(), partial("some function attributes/comments/grants are not emitted on create"), partial("identity-changing alterations require explicit migration"), yes()}},
	{TriggerFamily, true, Pipeline{partial("WHEN, arguments, enabled state, and complete function identity are not captured"), partial("WHEN, arguments, and enabled state are incomplete"), yes(), yes(), partial("WHEN, arguments, and enabled state are not emitted"), partial("implemented through drop/create"), yes()}},
	{GrantOwnershipFamily, true, Pipeline{partial("only modeled relation/function ACLs and owners are captured"), partial("only modeled object families are attached"), yes(), yes(), partial("create-time owner/grant emission is incomplete"), partial("selected owner and grant changes are supported"), partial("standalone grant/owner objects are not dropped")}},
	{PolicyFamily, false, Pipeline{yes(), yes(), yes(), yes(), yes(), partial("implemented through drop/create"), yes()}},
}

func Lookup(family Family) (Entry, bool) {
	for _, entry := range Matrix {
		if entry.Family == family {
			return entry, true
		}
	}
	return Entry{}, false
}

// ValidateMatrix enforces complete, unique registration and meaningful notes
// for every non-supported capability.
func ValidateMatrix() error {
	want := make(map[Family]bool, len(V1Families)+1)
	for _, family := range V1Families {
		want[family] = true
	}
	want[PolicyFamily] = false

	seen := make(map[Family]bool, len(Matrix))
	for _, entry := range Matrix {
		required, ok := want[entry.Family]
		if !ok {
			return fmt.Errorf("unregistered object family %q in capability matrix", entry.Family)
		}
		if seen[entry.Family] {
			return fmt.Errorf("duplicate object family %q in capability matrix", entry.Family)
		}
		seen[entry.Family] = true
		if entry.Required != required {
			return fmt.Errorf("object family %q required=%t, want %t", entry.Family, entry.Required, required)
		}
		for _, stage := range Stages {
			status := entry.Pipeline.At(stage)
			switch status.Level {
			case Supported:
				if status.Note != "" {
					return fmt.Errorf("%s/%s is supported but has a limitation note", entry.Family, stage)
				}
			case Partial, Unsupported:
				if status.Note == "" {
					return fmt.Errorf("%s/%s is %s without an explanation", entry.Family, stage, status.Level)
				}
			default:
				return fmt.Errorf("%s/%s has no support status", entry.Family, stage)
			}
		}
	}

	for family := range want {
		if !seen[family] {
			return fmt.Errorf("object family %q is missing from capability matrix", family)
		}
	}
	return nil
}

// UnsupportedError is the common fail-closed error for any pipeline stage.
// Feature should name the precise variant that could not be handled.
type UnsupportedError struct {
	Stage       Stage
	Family      Family
	Feature     string
	Object      schema.ObjectKey
	Reason      string
	Remediation string
}

func (e *UnsupportedError) Error() string {
	message := fmt.Sprintf("unsupported %s capability for %s", e.Stage, e.Family)
	if e.Feature != "" {
		message += ": " + e.Feature
	}
	if e.Object.Kind != "" {
		message += fmt.Sprintf(" (object %s/%s/%s)", e.Object.Kind, e.Object.Schema, e.Object.Name)
	}
	if e.Reason != "" {
		message += ": " + e.Reason
	}
	if e.Remediation != "" {
		message += "; remediation: " + e.Remediation
	}
	return message
}

func Require(family Family, stage Stage, feature string, object schema.ObjectKey) error {
	entry, ok := Lookup(family)
	if !ok {
		return &UnsupportedError{Stage: stage, Family: family, Feature: feature, Object: object, Reason: "object family is not registered"}
	}
	status := entry.Pipeline.At(stage)
	if status.Level == Supported {
		return nil
	}
	return &UnsupportedError{Stage: stage, Family: family, Feature: feature, Object: object, Reason: status.Note}
}

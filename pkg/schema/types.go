package schema

// Core type aliases for strong typing
type SchemaName string
type TableName string
type ColumnName string
type TypeName string
type Expr string

// QualifiedName represents a schema-qualified object reference
type QualifiedName struct {
	Schema SchemaName
	Name   string
}

// ObjectKey uniquely identifies a database object for the map-based diff algorithm
type ObjectKey struct {
	Kind       ObjectKind
	Schema     SchemaName
	Name       string
	TableName  TableName  // For columns, indexes, constraints, triggers, policies
	ColumnName ColumnName // For columns
	Signature  string     // For functions
}

type ObjectKind string

const (
	SchemaKind     ObjectKind = "schema"
	ExtensionKind  ObjectKind = "extension"
	TypeKind       ObjectKind = "type"
	SequenceKind   ObjectKind = "sequence"
	TableKind      ObjectKind = "table"
	ColumnKind     ObjectKind = "column"
	IndexKind      ObjectKind = "index"
	ConstraintKind ObjectKind = "constraint"
	ViewKind       ObjectKind = "view"
	FunctionKind   ObjectKind = "function"
	TriggerKind    ObjectKind = "trigger"
	PolicyKind     ObjectKind = "policy"
	GrantKind      ObjectKind = "grant"
	OwnerKind      ObjectKind = "owner"
)

// HashedObject wraps an object with its hash for efficient comparison
type HashedObject struct {
	Hash    string
	Payload DatabaseObject
}

// DatabaseObject is a sum type for all database objects we track
type DatabaseObject interface {
	isDatabaseObject()
	GetObjectKind() ObjectKind
}

// Schema represents a database schema
type Schema struct {
	Name    SchemaName
	Owner   *string
	Comment *string
}

func (Schema) isDatabaseObject()         {}
func (Schema) GetObjectKind() ObjectKind { return SchemaKind }

// Extension represents a Postgres extension
type Extension struct {
	Schema  SchemaName
	Name    string
	Version *string
}

func (Extension) isDatabaseObject()         {}
func (Extension) GetObjectKind() ObjectKind { return ExtensionKind }

// DBType represents database types (enums, domains, composites)
type DBType interface {
	isDatabaseObject()
	GetObjectKind() ObjectKind
	isDBType()
}

// EnumDef represents an enum type
type EnumDef struct {
	Schema  SchemaName
	Name    TypeName
	Values  []string // Order matters!
	Comment *string
}

func (EnumDef) isDatabaseObject()         {}
func (EnumDef) GetObjectKind() ObjectKind { return TypeKind }
func (EnumDef) isDBType()                 {}

// DomainDef represents a domain type
type DomainDef struct {
	Schema   SchemaName
	Name     TypeName
	BaseType TypeName
	NotNull  bool
	Default  *Expr
	Check    *Expr
	Comment  *string
}

func (DomainDef) isDatabaseObject()         {}
func (DomainDef) GetObjectKind() ObjectKind { return TypeKind }
func (DomainDef) isDBType()                 {}

// CompositeDef represents a composite type
type CompositeDef struct {
	Schema     SchemaName
	Name       TypeName
	Attributes []CompositeAttr
	Comment    *string
}

type CompositeAttr struct {
	Name string
	Type TypeName
}

func (CompositeDef) isDatabaseObject()         {}
func (CompositeDef) GetObjectKind() ObjectKind { return TypeKind }
func (CompositeDef) isDBType()                 {}

// Sequence represents a database sequence
type Sequence struct {
	Schema    SchemaName
	Name      string
	Owner     *string
	Type      string // "bigint", "integer", "smallint"
	Start     *int64
	Increment *int64
	MinValue  *int64
	MaxValue  *int64
	Cache     *int64
	Cycle     bool
	OwnedBy   *SequenceOwner
	Grants    []Grant
}

type SequenceOwner struct {
	Schema SchemaName
	Table  TableName
	Column ColumnName
}

func (Sequence) isDatabaseObject()         {}
func (Sequence) GetObjectKind() ObjectKind { return SequenceKind }

// Table represents a database table
type Table struct {
	Schema      SchemaName
	Name        TableName
	Owner       *string
	RelOptions  []string // Sorted reloptions like ["fillfactor=90"]
	Comment     *string
	Columns     []Column
	PrimaryKey  *PrimaryKey
	Uniques     []UniqueConstraint
	Checks      []CheckConstraint
	ForeignKeys []ForeignKey
	Partition   *PartitionSpec
	Inherits    []QualifiedName
	Grants      []Grant
}

func (Table) isDatabaseObject()         {}
func (Table) GetObjectKind() ObjectKind { return TableKind }

// Column represents a table column
type Column struct {
	Name      ColumnName
	Type      TypeName
	NotNull   bool
	Default   *Expr
	Generated *GeneratedSpec
	Identity  *IdentitySpec
	Collation *string
	Comment   *string
}

type GeneratedSpec struct {
	Expr   Expr
	Stored bool // True for STORED, False for VIRTUAL
}

type IdentitySpec struct {
	Always          bool // True for ALWAYS, False for BY DEFAULT
	SequenceOptions []SequenceOption
}

type SequenceOption struct {
	Type     string // Normalized option keyword, e.g. "START WITH", "INCREMENT BY", "NO MINVALUE"
	Value    int64  // Numeric option value when HasValue is true
	HasValue bool   // Whether this option carries an explicit numeric value
}

// PrimaryKey represents a primary key constraint
type PrimaryKey struct {
	Name              *string
	Cols              []ColumnName
	Deferrable        bool
	InitiallyDeferred bool
}

// UniqueConstraint represents a unique constraint
type UniqueConstraint struct {
	Name              string
	Cols              []ColumnName
	NullsDistinct     bool
	Deferrable        bool
	InitiallyDeferred bool
	NotValid          bool
}

// CheckConstraint represents a check constraint
type CheckConstraint struct {
	Name              string
	Expr              Expr
	NoInherit         bool
	Deferrable        bool
	InitiallyDeferred bool
	NotValid          bool
	// ColumnName is set for column-level CHECK constraints to track the associated column
	// Used for generating auto-names following PostgreSQL's pattern: {table}_{column}_check
	ColumnName *ColumnName
}

// ForeignKey represents a foreign key constraint
type ForeignKey struct {
	Name              string
	Cols              []ColumnName
	Ref               ForeignKeyRef
	OnUpdate          ReferentialAction
	OnDelete          ReferentialAction
	Match             MatchType
	Deferrable        bool
	InitiallyDeferred bool
	NotValid          bool
}

type ForeignKeyRef struct {
	Schema SchemaName
	Table  TableName
	Cols   []ColumnName
}

type ReferentialAction string

const (
	NoAction   ReferentialAction = "NO ACTION"
	Restrict   ReferentialAction = "RESTRICT"
	Cascade    ReferentialAction = "CASCADE"
	SetNull    ReferentialAction = "SET NULL"
	SetDefault ReferentialAction = "SET DEFAULT"
)

type MatchType string

const (
	MatchSimple  MatchType = "SIMPLE"
	MatchFull    MatchType = "FULL"
	MatchPartial MatchType = "PARTIAL"
)

// PartitionSpec represents table partitioning
type PartitionSpec struct {
	Strategy PartitionStrategy
	Keys     []PartitionKey
}

type PartitionStrategy string

const (
	PartitionRange PartitionStrategy = "RANGE"
	PartitionList  PartitionStrategy = "LIST"
	PartitionHash  PartitionStrategy = "HASH"
)

type PartitionKey struct {
	Expr      *Expr
	ColName   *ColumnName
	Collation *string
	OpClass   *string
}

// Index represents a database index
type Index struct {
	Schema    SchemaName
	Table     TableName
	Name      string
	Unique    bool
	Method    IndexMethod
	KeyExprs  []IndexKeyExpr
	Predicate *Expr
	Include   []ColumnName
	Comment   *string
}

func (Index) isDatabaseObject()         {}
func (Index) GetObjectKind() ObjectKind { return IndexKind }

type IndexMethod string

const (
	BTree  IndexMethod = "btree"
	Hash   IndexMethod = "hash"
	GiST   IndexMethod = "gist"
	SpGiST IndexMethod = "spgist"
	GIN    IndexMethod = "gin"
	BRIN   IndexMethod = "brin"
)

type IndexKeyExpr struct {
	Expr          Expr
	Collation     *string
	OpClass       *string
	Ordering      *IndexOrdering
	NullsOrdering *NullsOrdering
}

type IndexOrdering string
type NullsOrdering string

const (
	Asc  IndexOrdering = "ASC"
	Desc IndexOrdering = "DESC"

	NullsFirst NullsOrdering = "NULLS FIRST"
	NullsLast  NullsOrdering = "NULLS LAST"
)

// View represents a database view
type View struct {
	Schema          SchemaName
	Name            string
	Owner           *string
	Type            ViewType
	SecurityBarrier bool
	CheckOption     *CheckOption
	Comment         *string
	Definition      ViewDefinition
	Grants          []Grant
}

func (View) isDatabaseObject()         {}
func (View) GetObjectKind() ObjectKind { return ViewKind }

type ViewType string

const (
	RegularView      ViewType = "view"
	MaterializedView ViewType = "materialized view"
)

type CheckOption string

const (
	CheckLocal    CheckOption = "LOCAL"
	CheckCascaded CheckOption = "CASCADED"
)

type ViewDefinition struct {
	Query         string
	Dependencies  []ObjectReference
	OutputColumns []OutputColumn
}

type ObjectReference struct {
	Kind   ObjectKind
	Schema SchemaName
	Name   string
}

type OutputColumn struct {
	Name string
	Type TypeName
}

// Function represents a database function
type Function struct {
	Schema          SchemaName
	Name            string
	Owner           *string
	Args            []FunctionArg
	Returns         FunctionReturn
	Language        Language
	Volatility      Volatility
	Strict          bool
	SecurityDefiner bool
	SearchPath      []SchemaName
	Body            string
	Parallel        ParallelSafety
	Comment         *string
	Grants          []Grant
}

func (Function) isDatabaseObject()         {}
func (Function) GetObjectKind() ObjectKind { return FunctionKind }

type FunctionArg struct {
	Mode    ArgMode
	Name    *string
	Type    TypeName
	Default *Expr
}

type ArgMode string

const (
	InMode       ArgMode = "IN"
	OutMode      ArgMode = "OUT"
	InOutMode    ArgMode = "INOUT"
	VariadicMode ArgMode = "VARIADIC"
	TableMode    ArgMode = "TABLE"
)

type FunctionReturn interface {
	isFunctionReturn()
}

type ReturnsType struct {
	Type TypeName
}

func (ReturnsType) isFunctionReturn() {}

type ReturnsTable struct {
	Columns []TableColumn
}

type TableColumn struct {
	Name string
	Type TypeName
}

func (ReturnsTable) isFunctionReturn() {}

type ReturnsSetOf struct {
	Type TypeName
}

func (ReturnsSetOf) isFunctionReturn() {}

type Language string

const (
	SQL      Language = "sql"
	PlpgSQL  Language = "plpgsql"
	C        Language = "c"
	Internal Language = "internal"
)

type Volatility string

const (
	Immutable Volatility = "IMMUTABLE"
	Stable    Volatility = "STABLE"
	Volatile  Volatility = "VOLATILE"
)

type ParallelSafety string

const (
	ParallelSafe       ParallelSafety = "SAFE"
	ParallelRestricted ParallelSafety = "RESTRICTED"
	ParallelUnsafe     ParallelSafety = "UNSAFE"
)

// Trigger represents a database trigger
type Trigger struct {
	Schema     SchemaName
	Table      TableName
	Name       string
	Timing     TriggerTiming
	Events     []TriggerEvent
	ForEachRow bool
	When       *Expr
	Function   QualifiedName
	Enabled    TriggerEnabled
}

func (Trigger) isDatabaseObject()         {}
func (Trigger) GetObjectKind() ObjectKind { return TriggerKind }

type TriggerTiming string

const (
	Before    TriggerTiming = "BEFORE"
	After     TriggerTiming = "AFTER"
	InsteadOf TriggerTiming = "INSTEAD OF"
)

type TriggerEvent string

const (
	Insert   TriggerEvent = "INSERT"
	Update   TriggerEvent = "UPDATE"
	Delete   TriggerEvent = "DELETE"
	Truncate TriggerEvent = "TRUNCATE"
)

type TriggerEnabled string

const (
	EnabledAlways  TriggerEnabled = "ALWAYS"
	EnabledReplica TriggerEnabled = "REPLICA"
	Disabled       TriggerEnabled = "DISABLED"
)

// Policy represents a row-level security policy
type Policy struct {
	Schema     SchemaName
	Table      TableName
	Name       string
	Permissive bool // True for PERMISSIVE, False for RESTRICTIVE
	For        PolicyFor
	To         []string // Role names
	Using      *Expr
	WithCheck  *Expr
}

func (Policy) isDatabaseObject()         {}
func (Policy) GetObjectKind() ObjectKind { return PolicyKind }

type PolicyFor string

const (
	ForAll    PolicyFor = "ALL"
	ForSelect PolicyFor = "SELECT"
	ForInsert PolicyFor = "INSERT"
	ForUpdate PolicyFor = "UPDATE"
	ForDelete PolicyFor = "DELETE"
)

// Grant represents object permissions
type Grant struct {
	Grantee    string
	Privileges []Privilege
	Grantable  bool
}

type Privilege string

const (
	PrivSelect     Privilege = "SELECT"
	PrivInsert     Privilege = "INSERT"
	PrivUpdate     Privilege = "UPDATE"
	PrivDelete     Privilege = "DELETE"
	PrivTruncate   Privilege = "TRUNCATE"
	PrivReferences Privilege = "REFERENCES"
	PrivTrigger    Privilege = "TRIGGER"
	PrivExecute    Privilege = "EXECUTE"
	PrivUsage      Privilege = "USAGE"
	PrivCreate     Privilege = "CREATE"
	PrivConnect    Privilege = "CONNECT"
	PrivTemporary  Privilege = "TEMPORARY"
	PrivAll        Privilege = "ALL"
)

// SchemaObjectMap is used for the diff algorithm
type SchemaObjectMap map[ObjectKey]HashedObject

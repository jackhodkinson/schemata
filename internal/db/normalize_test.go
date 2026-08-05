package db

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestDetectSerialType(t *testing.T) {
	tests := []struct {
		name           string
		colType        schema.TypeName
		defaultExpr    schema.Expr
		sequence       schema.Sequence
		expectedSerial schema.TypeName
	}{
		{
			name:           "INTEGER with nextval and owned sequence should be SERIAL",
			colType:        "integer",
			defaultExpr:    "nextval('users_id_seq'::regclass)",
			sequence:       canonicalSerialSequence("public", "users", "id", "integer", 2147483647),
			expectedSerial: "serial",
		},
		{
			name:           "BIGINT with nextval and owned sequence should be BIGSERIAL",
			colType:        "bigint",
			defaultExpr:    "nextval('orders_id_seq'::regclass)",
			sequence:       canonicalSerialSequence("public", "orders", "id", "bigint", 9223372036854775807),
			expectedSerial: "bigserial",
		},
		{
			name:           "SMALLINT with nextval and owned sequence should be SMALLSERIAL",
			colType:        "smallint",
			defaultExpr:    "nextval('items_id_seq'::regclass)",
			sequence:       canonicalSerialSequence("public", "items", "id", "smallint", 32767),
			expectedSerial: "smallserial",
		},
		{
			name:           "INTEGER with a BIGINT backing sequence is not SERIAL",
			colType:        "integer",
			defaultExpr:    "nextval('users_id_seq'::regclass)",
			sequence:       canonicalSerialSequence("public", "users", "id", "bigint", 9223372036854775807),
			expectedSerial: "",
		},
		{
			name:           "INTEGER with non-nextval default should NOT be SERIAL",
			colType:        "integer",
			defaultExpr:    "42",
			sequence:       canonicalSerialSequence("public", "users", "id", "integer", 2147483647),
			expectedSerial: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner := "postgres"
			tableName := tt.sequence.OwnedBy.Table
			table := schema.Table{
				Schema: tt.sequence.OwnedBy.Schema,
				Name:   tableName,
				Owner:  &owner,
				Columns: []schema.Column{{
					Name:    tt.sequence.OwnedBy.Column,
					Type:    tt.colType,
					Default: &tt.defaultExpr,
				}},
			}
			normalized := NormalizeTable(table, []schema.Sequence{tt.sequence})
			if tt.expectedSerial == "" {
				assert.Equal(t, schema.NormalizeTypeName(tt.colType), normalized.Columns[0].Type)
				assert.NotNil(t, normalized.Columns[0].Default)
			} else {
				assert.Equal(t, tt.expectedSerial, normalized.Columns[0].Type)
				assert.Nil(t, normalized.Columns[0].Default)
			}
		})
	}
}

func TestNormalizeTable(t *testing.T) {
	// Test full table normalization
	sequences := []schema.Sequence{canonicalSerialSequence("public", "users", "id", "integer", 2147483647)}

	defaultExpr := schema.Expr("nextval('users_id_seq'::regclass)")
	table := schema.Table{
		Schema: "public",
		Name:   "users",
		Owner:  stringPointer("postgres"),
		Columns: []schema.Column{
			{
				Name:    "id",
				Type:    "integer",
				NotNull: true,
				Default: &defaultExpr,
			},
			{
				Name:    "name",
				Type:    "character varying(255)",
				NotNull: false,
			},
		},
	}

	normalized := NormalizeTable(table, sequences)

	// Check that id column was normalized to SERIAL
	assert.Equal(t, schema.TypeName("serial"), normalized.Columns[0].Type)
	assert.Nil(t, normalized.Columns[0].Default, "SERIAL column should have nil default")

	// Check that name column was type-normalized
	assert.Equal(t, schema.TypeName("varchar(255)"), normalized.Columns[1].Type)
}

func TestNormalizeCatalogTablePreservesNoncanonicalSerialBackingSequences(t *testing.T) {
	t.Parallel()

	owner := "postgres"
	canonical := canonicalSerialSequence("public", "users", "id", "integer", 2147483647)
	canonicalDefault := schema.Expr("nextval('users_id_seq'::regclass)")

	tests := map[string]struct {
		mutate      func(*schema.Sequence)
		defaultExpr schema.Expr
	}{
		"custom name": {
			mutate:      func(seq *schema.Sequence) { seq.Name = "custom_ids" },
			defaultExpr: "nextval('custom_ids'::regclass)",
		},
		"custom cache": {
			mutate:      func(seq *schema.Sequence) { value := int64(9); seq.Cache = &value },
			defaultExpr: canonicalDefault,
		},
		"comment": {
			mutate:      func(seq *schema.Sequence) { value := "important"; seq.Comment = &value },
			defaultExpr: canonicalDefault,
		},
		"custom ACL": {
			mutate: func(seq *schema.Sequence) {
				seq.Grants = append(seq.Grants, schema.Grant{Grantee: schema.PublicGrantee(), Privileges: []schema.Privilege{schema.PrivSelect}})
			},
			defaultExpr: canonicalDefault,
		},
		"near-miss default reference": {
			mutate:      func(*schema.Sequence) {},
			defaultExpr: "nextval('not_users_id_seq'::regclass)",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			seq := canonical
			test.mutate(&seq)
			table := schema.Table{
				Schema:  "public",
				Name:    "users",
				Owner:   &owner,
				Columns: []schema.Column{{Name: "id", Type: "integer", Default: &test.defaultExpr}},
			}

			normalized, collapsed := normalizeCatalogTable(table, []catalogSequence{{Sequence: seq, DependencyKind: sequenceSerial}})

			assert.Empty(t, collapsed)
			assert.Equal(t, schema.TypeName("integer"), normalized.Columns[0].Type)
			assert.NotNil(t, normalized.Columns[0].Default)
		})
	}
}

func TestNormalizeTypeName(t *testing.T) {
	tests := []struct {
		input    schema.TypeName
		expected schema.TypeName
	}{
		{"int", "integer"},
		{"int4", "integer"},
		{"int8", "bigint"},
		{"int2", "smallint"},
		{"bool", "boolean"},
		{"character varying(255)", "varchar(255)"},
		{"character varying", "varchar"},
		{"character(10)", "char(10)"},
		{"character", "char"},
		{"text", "text"}, // Should stay as-is
		{"uuid", "uuid"}, // Should stay as-is
		{"pg_catalog.int4", "integer"},
		{"public.value_type", "value_type"},
		{"tenant.value_type[]", "tenant.value_type[]"},
		{`"Tenant"."ValueType"`, `"Tenant"."ValueType"`},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := schema.NormalizeTypeName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReferencesSequence(t *testing.T) {
	tests := []struct {
		name        string
		expr        schema.Expr
		seqSchema   schema.SchemaName
		seqName     string
		shouldMatch bool
	}{
		{
			name:        "Unqualified sequence name",
			expr:        "nextval('users_id_seq'::regclass)",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: true,
		},
		{
			name:        "Qualified sequence name",
			expr:        "nextval('public.users_id_seq'::regclass)",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: true,
		},
		{
			name:        "Quoted qualified sequence name",
			expr:        "nextval('\"public\".\"users_id_seq\"'::regclass)",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: true,
		},
		{
			name:        "Different sequence name",
			expr:        "nextval('orders_id_seq'::regclass)",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: false,
		},
		{
			name:        "Containing sequence name is not an exact match",
			expr:        "nextval('not_users_id_seq'::regclass)",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: false,
		},
		{
			name:        "Compound expression is not the SERIAL expansion",
			expr:        "nextval('users_id_seq'::regclass) + 1",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: false,
		},
		{
			name:        "Nested call is not the SERIAL expansion",
			expr:        "coalesce(nextval('users_id_seq'::regclass), 1)",
			seqSchema:   "public",
			seqName:     "users_id_seq",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := referencesSequence(tt.expr, tt.seqSchema, tt.seqName)
			assert.Equal(t, tt.shouldMatch, result)
		})
	}
}

func canonicalSerialSequence(schemaName schema.SchemaName, table schema.TableName, column schema.ColumnName, sequenceType string, max int64) schema.Sequence {
	owner := "postgres"
	start := int64(1)
	increment := int64(1)
	min := int64(1)
	cache := int64(1)
	return schema.Sequence{
		Schema:    schemaName,
		Name:      string(table) + "_" + string(column) + "_seq",
		Owner:     &owner,
		Type:      sequenceType,
		Start:     &start,
		Increment: &increment,
		MinValue:  &min,
		MaxValue:  &max,
		Cache:     &cache,
		OwnedBy:   &schema.SequenceOwner{Schema: schemaName, Table: table, Column: column},
		Grants: []schema.Grant{{
			Grantee:    schema.RoleGrantee(owner),
			Privileges: []schema.Privilege{schema.PrivSelect, schema.PrivUpdate, schema.PrivUsage},
		}},
	}
}

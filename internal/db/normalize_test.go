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
		sequences      []schema.Sequence
		expectedSerial schema.TypeName
	}{
		{
			name:        "INTEGER with nextval and owned sequence should be SERIAL",
			colType:     "integer",
			defaultExpr: "nextval('users_id_seq'::regclass)",
			sequences: []schema.Sequence{
				{
					Schema: "public",
					Name:   "users_id_seq",
					Type:   "bigint",
					OwnedBy: &schema.SequenceOwner{
						Schema: "public",
						Table:  "users",
						Column: "id",
					},
				},
			},
			expectedSerial: "serial",
		},
		{
			name:        "BIGINT with nextval and owned sequence should be BIGSERIAL",
			colType:     "bigint",
			defaultExpr: "nextval('orders_id_seq'::regclass)",
			sequences: []schema.Sequence{
				{
					Schema: "public",
					Name:   "orders_id_seq",
					Type:   "bigint",
					OwnedBy: &schema.SequenceOwner{
						Schema: "public",
						Table:  "orders",
						Column: "id",
					},
				},
			},
			expectedSerial: "bigserial",
		},
		{
			name:        "SMALLINT with nextval and owned sequence should be SMALLSERIAL",
			colType:     "smallint",
			defaultExpr: "nextval('items_id_seq'::regclass)",
			sequences: []schema.Sequence{
				{
					Schema: "public",
					Name:   "items_id_seq",
					Type:   "bigint",
					OwnedBy: &schema.SequenceOwner{
						Schema: "public",
						Table:  "items",
						Column: "id",
					},
				},
			},
			expectedSerial: "smallserial",
		},
		{
			name:        "INTEGER with nextval but NO owned sequence should NOT be SERIAL",
			colType:     "integer",
			defaultExpr: "nextval('shared_seq'::regclass)",
			sequences: []schema.Sequence{
				{
					Schema:  "public",
					Name:    "shared_seq",
					Type:    "bigint",
					OwnedBy: nil, // Not owned
				},
			},
			expectedSerial: "",
		},
		{
			name:           "INTEGER with non-nextval default should NOT be SERIAL",
			colType:        "integer",
			defaultExpr:    "42",
			sequences:      []schema.Sequence{},
			expectedSerial: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build sequence map
			seqMap := make(map[string]schema.Sequence)
			for _, seq := range tt.sequences {
				if seq.OwnedBy != nil {
					key := string(seq.OwnedBy.Schema) + "." + string(seq.OwnedBy.Table) + "." + string(seq.OwnedBy.Column)
					seqMap[key] = seq
				}
			}

			// Extract table/column info from the first sequence's OwnedBy (if exists)
			tableSchema := schema.SchemaName("public")
			tableName := schema.TableName("users")
			colName := schema.ColumnName("id")

			if len(tt.sequences) > 0 && tt.sequences[0].OwnedBy != nil {
				tableSchema = tt.sequences[0].OwnedBy.Schema
				tableName = tt.sequences[0].OwnedBy.Table
				colName = tt.sequences[0].OwnedBy.Column
			}

			result := detectSerialType(tt.colType, tt.defaultExpr, tableSchema, tableName, colName, seqMap)
			assert.Equal(t, tt.expectedSerial, result)
		})
	}
}

func TestNormalizeTable(t *testing.T) {
	// Test full table normalization
	sequences := []schema.Sequence{
		{
			Schema: "public",
			Name:   "users_id_seq",
			Type:   "bigint",
			OwnedBy: &schema.SequenceOwner{
				Schema: "public",
				Table:  "users",
				Column: "id",
			},
		},
	}

	defaultExpr := schema.Expr("nextval('users_id_seq'::regclass)")
	table := schema.Table{
		Schema: "public",
		Name:   "users",
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
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := normalizeTypeName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReferencesSequence(t *testing.T) {
	tests := []struct {
		name         string
		expr         schema.Expr
		seqSchema    schema.SchemaName
		seqName      string
		shouldMatch  bool
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := referencesSequence(tt.expr, tt.seqSchema, tt.seqName)
			assert.Equal(t, tt.shouldMatch, result)
		})
	}
}

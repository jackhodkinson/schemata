package sqlrender

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdentifierAlwaysQuotesOneComponent(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"reserved word":    "select",
		"mixed case":       "CamelCase",
		"embedded quote":   `say"hello`,
		"embedded dot":     "tenant.data",
		"Unicode":          "顧客",
		"injection shaped": `x"; DROP SCHEMA public; --`,
	}
	wants := map[string]string{
		"reserved word":    `"select"`,
		"mixed case":       `"CamelCase"`,
		"embedded quote":   `"say""hello"`,
		"embedded dot":     `"tenant.data"`,
		"Unicode":          `"顧客"`,
		"injection shaped": `"x""; DROP SCHEMA public; --"`,
	}

	for name, input := range tests {
		name, input := name, input
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, ValidateIdentifier(input))
			assert.Equal(t, wants[name], Identifier(input))
		})
	}
}

func TestQualifiedNeverReinterpretsDotsInsideComponents(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `"tenant.data"."select"`, Qualified("tenant.data", "select"))
	assert.Equal(t, `"顧客", "a.b"`, IdentifierList([]string{"顧客", "a.b"}))
}

func TestParseQualifiedIsStrictAndCanonical(t *testing.T) {
	t.Parallel()

	valid := map[string]string{
		"simple":            `public.items`,
		"one component":     `en-US-x-icu`,
		"quoted dots":       `"odd.schema"."table.name"`,
		"quoted quote":      `public."say""hello"`,
		"Unicode":           `顧客.注文`,
		"injection as data": `public."x""; DROP SCHEMA public; --"`,
	}
	wants := map[string]string{
		"simple":            `"public"."items"`,
		"one component":     `"en-US-x-icu"`,
		"quoted dots":       `"odd.schema"."table.name"`,
		"quoted quote":      `"public"."say""hello"`,
		"Unicode":           `"顧客"."注文"`,
		"injection as data": `"public"."x""; DROP SCHEMA public; --"`,
	}
	for name, input := range valid {
		got, err := ParseQualified(input, 1, 2)
		require.NoError(t, err, name)
		assert.Equal(t, wants[name], got, name)
	}

	for _, input := range []string{"", ".a", "a.", "a..b", "a.b.c", `"unterminated`, `"a"junk`, " a", "a b", "a\x00b"} {
		_, err := ParseQualified(input, 1, 2)
		var invalid *InvalidValueError
		require.ErrorAs(t, err, &invalid, input)
	}
}

func TestRoleAlwaysRendersOrdinaryRoleIdentifier(t *testing.T) {
	t.Parallel()

	got, err := Role("PUBLIC")
	require.NoError(t, err)
	assert.Equal(t, `"PUBLIC"`, got)

	got, err = Role("public")
	require.NoError(t, err)
	assert.Equal(t, `"public"`, got)

	got, err = Role(`ops"; DROP ROLE admin; --`)
	require.NoError(t, err)
	assert.Equal(t, `"ops""; DROP ROLE admin; --"`, got)
}

func TestLiteralEscapesInjectionShapedValues(t *testing.T) {
	t.Parallel()

	got, err := Literal("it's'); DROP TABLE customers; --")
	require.NoError(t, err)
	assert.Equal(t, `'it''s''); DROP TABLE customers; --'`, got)

	got, err = Literal("顧客")
	require.NoError(t, err)
	assert.Equal(t, `'顧客'`, got)

	got, err = Literal("\\'); DROP TABLE customers; --")
	require.NoError(t, err)
	assert.Equal(t, `E'\\''); DROP TABLE customers; --'`, got)
}

func TestRenderRejectsUnrepresentableValues(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		ValidateIdentifier(""),
		ValidateIdentifier("a\x00b"),
		func() error { _, err := Literal("a\x00b"); return err }(),
		func() error { _, err := Role("a\x00b"); return err }(),
	} {
		var invalid *InvalidValueError
		require.True(t, errors.As(err, &invalid), "%v", err)
	}
}

func TestNextvalRegclassReferenceIsExact(t *testing.T) {
	t.Parallel()

	reference, qualified, ok := NextvalRegclassReference(`pg_catalog.nextval('"Sequence.Schema"."Odd IDs"'::pg_catalog.regclass)`)
	require.True(t, ok)
	assert.True(t, qualified)
	assert.Equal(t, `"Sequence.Schema"."Odd IDs"`, reference)

	reference, qualified, ok = NextvalRegclassReference(`nextval('item_ids'::regclass)`)
	require.True(t, ok)
	assert.False(t, qualified)
	assert.Equal(t, `"item_ids"`, reference)

	reference, qualified, ok = NextvalRegclassReference(`nextval('"Odd.IDs"'::regclass)`)
	require.True(t, ok)
	assert.False(t, qualified, "a dot inside one quoted identifier is not schema qualification")
	assert.Equal(t, `"Odd.IDs"`, reference)

	reference, qualified, ok = NextvalRegclassReference(`NEXTVAL('PUBLIC.Item_IDs'::REGCLASS)`)
	require.True(t, ok)
	assert.True(t, qualified)
	assert.Equal(t, `"public"."item_ids"`, reference, "unquoted regclass identifiers fold to lower case")

	reference, qualified, ok = NextvalRegclassReference(`nextval('"PUBLIC"."Item_IDs"'::regclass)`)
	require.True(t, ok)
	assert.True(t, qualified)
	assert.Equal(t, `"PUBLIC"."Item_IDs"`, reference, "quoted regclass identifiers preserve case")

	for _, expression := range []string{
		`nextval('item_ids'::text)`,
		`nextval('item_ids'::evil.regclass)`,
		`other.nextval('item_ids'::regclass)`,
		`pg_catalog."NEXTVAL"('item_ids'::regclass)`,
		`nextval('item_ids'::pg_catalog."REGCLASS")`,
		`nextval('item_ids'::regclass) + 1`,
		`nextval('not_item_ids'::regclass)`,
		`nextval('a.b.c'::regclass)`,
	} {
		_, _, ok := NextvalRegclassReference(expression)
		if expression == `nextval('not_item_ids'::regclass)` {
			assert.True(t, ok, "a different exact sequence name is still a valid reference")
			continue
		}
		assert.False(t, ok, expression)
	}
}

func TestNextvalRegclassReferencesWalksNestedExpression(t *testing.T) {
	t.Parallel()

	expression := `coalesce(
		nextval('sequences.first_ids'::regclass),
		abs(nextval('sequences.second_ids'::pg_catalog.regclass)),
		nextval('sequences.first_ids'::regclass)
	) + nextval('unqualified_ids'::regclass)`

	_, _, exact := NextvalRegclassReference(expression)
	assert.False(t, exact, "the exact-root helper must remain strict for SERIAL normalization")
	assert.Equal(t, []NextvalReference{
		{Reference: `"sequences"."first_ids"`, Qualified: true},
		{Reference: `"sequences"."second_ids"`, Qualified: true},
		{Reference: `"unqualified_ids"`, Qualified: false},
	}, NextvalRegclassReferences(expression))
}

func TestNextvalRegclassReferencesRejectsNestedNearMisses(t *testing.T) {
	t.Parallel()

	expression := `coalesce(
		other.nextval('sequences.item_ids'::regclass),
		nextval('sequences.item_ids'::text),
		nextval('sequences.item_ids'::evil.regclass),
		length('nextval(''sequences.item_ids''::regclass)'),
		nextval('sequences.not_item_ids'::regclass)
	)`

	assert.Equal(t, []NextvalReference{{
		Reference: `"sequences"."not_item_ids"`,
		Qualified: true,
	}}, NextvalRegclassReferences(expression))
}

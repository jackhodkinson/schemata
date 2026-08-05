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

func TestRoleDistinguishesPublicSentinelFromQuotedRole(t *testing.T) {
	t.Parallel()

	got, err := Role("PUBLIC")
	require.NoError(t, err)
	assert.Equal(t, "PUBLIC", got)

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

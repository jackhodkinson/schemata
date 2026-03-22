package differ

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortAlterChanges_OrderStable(t *testing.T) {
	ch := []string{
		"comment changed",
		"owner changed",
		"add column z",
		"reloptions changed",
		"add grant\talice\tSELECT\tfalse",
	}
	SortAlterChanges(ch)
	assert.Equal(t, "owner changed", ch[0])
	assert.Equal(t, "add column z", ch[1])
	assert.Equal(t, "reloptions changed", ch[2])
	assert.Equal(t, "comment changed", ch[3])
	assert.Equal(t, "add grant\talice\tSELECT\tfalse", ch[4])
}

func TestSortAlterChanges_DeterministicAcrossRuns(t *testing.T) {
	base := []string{
		"drop foreign key fk1",
		"add column a",
		"owner changed",
		"alter column b: type changed from int to bigint",
	}
	var last []string
	for i := 0; i < 20; i++ {
		cp := append([]string(nil), base...)
		SortAlterChanges(cp)
		if last != nil {
			assert.Equal(t, last, cp)
		}
		last = cp
	}
}

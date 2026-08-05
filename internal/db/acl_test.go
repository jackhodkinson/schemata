package db

import (
	"testing"

	"github.com/jackhodkinson/schemata/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrivilegeFromACLIncludesMaintain(t *testing.T) {
	t.Parallel()

	privilege, err := privilegeFromACL("MAINTAIN")
	require.NoError(t, err)
	assert.Equal(t, schema.PrivMaintain, privilege)
}

func TestPrivilegeFromACLRejectsUnknownPrivileges(t *testing.T) {
	t.Parallel()

	_, err := privilegeFromACL("FUTURE_PRIVILEGE")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to omit")
}

package cli

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/config"
)

// requirePinnedProductionTarget prevents every target migration path,
// including dry-run planning, from connecting without an independently
// configured database/cluster identity. Development connections are excluded
// deliberately: they are selected through the separate --dev workflow.
func requirePinnedProductionTarget(target *config.DBConnection, operation string) error {
	if target == nil || target.Identity == nil {
		return fmt.Errorf(
			"%s requires the selected target to pin identity.database and identity.system-identifier; obtain both through an independently verified administrative connection",
			operation,
		)
	}
	if err := target.Identity.Validate(); err != nil {
		return fmt.Errorf("%s has an invalid selected target identity: %w", operation, err)
	}
	return nil
}

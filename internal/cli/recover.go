package cli

import (
	"fmt"

	"github.com/jackhodkinson/schemata/internal/app"
	"github.com/jackhodkinson/schemata/internal/config"
	"github.com/jackhodkinson/schemata/internal/db"
	"github.com/jackhodkinson/schemata/internal/migration"
	"github.com/spf13/cobra"
)

var (
	recoverTarget           string
	recoverDev              bool
	recoverRetry            bool
	recoverMarkApplied      bool
	recoverConfirmedThrough int
	recoverConfirmDatabase  string
	recoverConfirmSystemID  string
)

var recoverCmd = &cobra.Command{
	Use:   "recover VERSION",
	Short: "Explicitly reconcile an incomplete non-transactional migration",
	Long: `Recover exactly one interrupted or failed non-transactional migration.

Normal apply and migrate commands never resume partial migrations implicitly.
Before retrying, inspect the database and attest how many leading statements
are already durable with --confirmed-through. Use --mark-applied only after
manually verifying that every statement in the migration has completed.

Examples:
  schemata recover 20231015120530 --target prod --retry --confirmed-through 2 \
    --confirm-database app --confirm-system-identifier 7561860200789946402
  schemata recover 20231015120530 --target prod --mark-applied \
    --confirm-database app --confirm-system-identifier 7561860200789946402
`,
	Args: cobra.ExactArgs(1),
	RunE: runRecover,
}

func init() {
	recoverCmd.Flags().StringVar(&recoverTarget, "target", "", "Target name containing the incomplete migration")
	recoverCmd.Flags().BoolVar(&recoverDev, "dev", false, "Recover the configured development database")
	recoverCmd.Flags().BoolVar(&recoverRetry, "retry", false, "Resume after the explicitly confirmed statement boundary")
	recoverCmd.Flags().BoolVar(&recoverMarkApplied, "mark-applied", false, "Attest that every statement is already durable and mark the migration applied")
	recoverCmd.Flags().IntVar(&recoverConfirmedThrough, "confirmed-through", -1, "Number of leading statements verified as durable (required with --retry)")
	recoverCmd.Flags().StringVar(&recoverConfirmDatabase, "confirm-database", "", "Confirm the configured target database name")
	recoverCmd.Flags().StringVar(&recoverConfirmSystemID, "confirm-system-identifier", "", "Confirm the configured PostgreSQL cluster system identifier")
}

func runRecover(cmd *cobra.Command, args []string) error {
	if recoverTarget == "" && !recoverDev {
		return fmt.Errorf("must specify either --target or --dev")
	}
	if recoverTarget != "" && recoverDev {
		return fmt.Errorf("cannot specify both --target and --dev")
	}
	confirmedChanged := cmd.Flags().Changed("confirmed-through")
	options, err := buildRecoveryOptions(
		args[0],
		recoverRetry,
		recoverMarkApplied,
		confirmedChanged,
		recoverConfirmedThrough,
	)
	if err != nil {
		return err
	}

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	var targetConn *config.DBConnection
	if recoverDev {
		if cfg.Dev == nil {
			return fmt.Errorf("dev database not configured")
		}
		targetConn = cfg.Dev
	} else {
		targetConn, err = cfg.GetTargetByName(recoverTarget)
		if err != nil {
			return err
		}
	}
	if err := confirmRecoveryTargetIdentity(
		targetConn,
		recoverConfirmDatabase,
		recoverConfirmSystemID,
	); err != nil {
		return err
	}

	service := app.NewService(allowCascade)
	migrations, err := service.ScanMigrations(cfg.Migrations.GetDir(), cfg.Migrations.GetFormat())
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	pool, err := db.Connect(ctx, targetConn, db.WithDatabaseConfig(cfg.Database))
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	if recoverRetry {
		fmt.Printf(
			"Retrying migration %s on database %s (system %s) after operator-confirmed statement %d...\n",
			options.Version,
			recoverConfirmDatabase,
			recoverConfirmSystemID,
			*options.ConfirmedThrough,
		)
	} else {
		fmt.Printf(
			"Marking migration %s applied on database %s (system %s) after operator attestation...\n",
			options.Version,
			recoverConfirmDatabase,
			recoverConfirmSystemID,
		)
	}

	if err := migration.NewApplier(pool, false).Recover(ctx, migrations, options); err != nil {
		return fmt.Errorf("failed to recover migration: %w", err)
	}
	fmt.Printf("Migration %s recovered successfully\n", options.Version)
	return nil
}

func confirmRecoveryTargetIdentity(
	target *config.DBConnection,
	confirmedDatabase string,
	confirmedSystemIdentifier string,
) error {
	if target == nil || target.Identity == nil {
		return fmt.Errorf(
			"recovery requires database identity pinning on the selected connection; configure identity.database and identity.system-identifier first",
		)
	}
	if confirmedDatabase == "" || confirmedSystemIdentifier == "" {
		return fmt.Errorf(
			"recovery requires --confirm-database and --confirm-system-identifier to match the selected target identity",
		)
	}
	if confirmedDatabase != target.Identity.Database {
		return fmt.Errorf(
			"recovery database confirmation mismatch: supplied %q, configured target is %q",
			confirmedDatabase,
			target.Identity.Database,
		)
	}

	confirmedIdentity := config.DatabaseIdentity{
		Database:         confirmedDatabase,
		SystemIdentifier: confirmedSystemIdentifier,
	}
	confirmedValue, err := confirmedIdentity.SystemIdentifierValue()
	if err != nil {
		return fmt.Errorf("invalid --confirm-system-identifier: %w", err)
	}
	expectedValue, err := target.Identity.SystemIdentifierValue()
	if err != nil {
		return fmt.Errorf("invalid configured target system identifier: %w", err)
	}
	if confirmedValue != expectedValue {
		return fmt.Errorf(
			"recovery system-identifier confirmation mismatch: supplied %s, configured target is %s",
			confirmedSystemIdentifier,
			target.Identity.SystemIdentifier,
		)
	}
	return nil
}

func buildRecoveryOptions(
	version string,
	retry bool,
	markApplied bool,
	confirmedChanged bool,
	confirmedThrough int,
) (migration.RecoveryOptions, error) {
	if retry == markApplied {
		return migration.RecoveryOptions{}, fmt.Errorf("must specify exactly one of --retry or --mark-applied")
	}
	if retry && !confirmedChanged {
		return migration.RecoveryOptions{}, fmt.Errorf("--retry requires --confirmed-through N after inspecting durable database state")
	}
	if markApplied && confirmedChanged {
		return migration.RecoveryOptions{}, fmt.Errorf("--confirmed-through is only valid with --retry")
	}
	if confirmedChanged && confirmedThrough < 0 {
		return migration.RecoveryOptions{}, fmt.Errorf("--confirmed-through must not be negative")
	}

	options := migration.RecoveryOptions{
		Version: version,
		Action:  migration.RecoveryActionMarkApplied,
	}
	if retry {
		confirmed := confirmedThrough
		options.Action = migration.RecoveryActionRetry
		options.ConfirmedThrough = &confirmed
	}
	return options, nil
}

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate databases to a specific schema version",
	Long: `Migrate databases from their current schema version to a target version.
The migration system applies schema updates iteratively.

Each migration step is registered in code and can perform schema transformations,
data migrations, or any other necessary updates. Migrations run on the opened
engine (after recovery) and take the data-directory lock, so this command fails
while a cachydb server is running on the same data directory.`,
	Example: `  cachydb utils migrate --list                       # registered migrations
  cachydb utils migrate --show-version --database main
  cachydb utils migrate --database main              # migrate one database to the latest version
  cachydb utils migrate --all --target 1             # migrate every database to version 1`,
	Args: cobra.NoArgs,
	RunE: runMigrate,
}

var (
	migrateDatabase string
	migrateAll      bool
	targetVersion   int
	showVersion     bool
	listMigrations  bool
)

func init() {
	utilsCmd.AddCommand(migrateCmd)

	migrateCmd.Flags().StringVarP(&migrateDatabase, "database", "d", "", "database to migrate")
	migrateCmd.Flags().BoolVarP(&migrateAll, "all", "a", false, "migrate every database")
	migrateCmd.Flags().IntVarP(&targetVersion, "target", "t", db.CurrentSchemaVersion, "target schema version")
	migrateCmd.Flags().BoolVarP(&showVersion, "show-version", "v", false, "print the schema version of --database")
	migrateCmd.Flags().BoolVarP(&listMigrations, "list", "l", false, "list the registered migrations (does not open the engine)")
}

// migrateParams are the parsed flags of the migrate command.
type migrateParams struct {
	database    string
	all         bool
	target      int
	showVersion bool
	list        bool
}

func runMigrate(cmd *cobra.Command, args []string) error {
	p := migrateParams{
		database:    migrateDatabase,
		all:         migrateAll,
		target:      targetVersion,
		showVersion: showVersion,
		list:        listMigrations,
	}
	if err := p.validate(); err != nil {
		return err
	}
	cmd.SilenceUsage = true
	return runMigration(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), engineOptions(), p)
}

// validate checks the flag combination and the target version.
func (p migrateParams) validate() error {
	// Listing migrations ignores every other flag.
	if p.list {
		return nil
	}

	if p.showVersion {
		if p.database == "" {
			return errors.New("--database is required when using --show-version. Use 'cachydb utils list' to see available databases")
		}
		return nil
	}

	if !p.all && p.database == "" {
		return errors.New("either --database or --all must be specified. Use 'cachydb utils list' to see available databases")
	}
	if p.all && p.database != "" {
		return errors.New("cannot specify both --database and --all")
	}
	return validateTarget(p.target)
}

// validateTarget rejects a target below 1, and a target above
// db.CurrentSchemaVersion unless a migration is registered for every step
// up to it.
func validateTarget(target int) error {
	if target < 1 {
		return fmt.Errorf("invalid target version %d: schema versions start at 1", target)
	}
	if target <= db.CurrentSchemaVersion {
		return nil
	}
	registered := db.ListMigrations()
	for v := db.CurrentSchemaVersion; v < target; v++ {
		if !slices.Contains(registered, v) {
			return fmt.Errorf(
				"invalid target version %d: the latest supported schema version is %d and no migration is registered from version %d to %d",
				target, db.CurrentSchemaVersion, v, v+1,
			)
		}
	}
	return nil
}

// runMigration executes the validated migrate command. Progress goes to
// w, warnings to errW.
func runMigration(ctx context.Context, w, errW io.Writer, opts db.Options, p migrateParams) error {
	out := &output{w: w}

	// List migrations (doesn't require the engine)
	if p.list {
		versions := db.ListMigrations()
		if len(versions) == 0 {
			out.printf("No migrations registered yet\n")
			return out.err
		}
		out.printf("Registered migrations:\n")
		for _, version := range versions {
			out.printf("  Version %d -> %d\n", version, version+1)
		}
		out.printf("\nCurrent schema version: %d\n", db.CurrentSchemaVersion)
		return out.err
	}

	if !p.showVersion && p.target > db.CurrentSchemaVersion {
		warn := &output{w: errW}
		warn.printf("Warning: migrating past schema version %d; this build cannot open a database at version %d\n",
			db.CurrentSchemaVersion, p.target)
		if warn.err != nil {
			return warn.err
		}
	}

	return withEngine(ctx, opts, func(e *db.Engine) error {
		// Show version
		if p.showVersion {
			database, err := e.Database(p.database)
			if err != nil {
				return fmt.Errorf("failed to get database version: %w", err)
			}
			out.printf("Database '%s' is at schema version %d\n", p.database, database.SchemaVersion())
			out.printf("Current schema version: %d\n", db.CurrentSchemaVersion)
			return out.err
		}

		// Migrate all databases
		if p.all {
			out.printf("Migrating all databases to version %d...\n", p.target)
			if err := e.MigrateAll(ctx, p.target); err != nil {
				return fmt.Errorf("migration failed: %w", err)
			}
			out.printf("All databases migrated successfully!\n")
			return out.err
		}

		// Migrate single database
		if err := e.MigrateDatabase(ctx, p.database, p.target); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		out.printf("Database '%s' migrated successfully!\n", p.database)
		return out.err
	})
}

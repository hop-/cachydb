package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate databases from JSON to binary format",
	Long: `Migrate existing JSON-based databases to the new binary storage format.
This command will:
1. Create a backup of the database
2. Load data from JSON format
3. Save data to binary format with compression
4. Verify the migration was successful`,
	RunE: runMigrate,
}

var (
	migrateDatabase string
	migrateAll      bool
	skipBackup      bool
	verifyOnly      bool
	restoreBackup   bool
)

func init() {
	rootCmd.AddCommand(migrateCmd)

	migrateCmd.Flags().StringVarP(&migrateDatabase, "database", "d", "", "Database name to migrate (required unless --all is specified)")
	migrateCmd.Flags().BoolVarP(&migrateAll, "all", "a", false, "Migrate all databases")
	migrateCmd.Flags().BoolVar(&skipBackup, "skip-backup", false, "Skip creating backup before migration")
	migrateCmd.Flags().BoolVar(&verifyOnly, "verify", false, "Only verify migration without migrating")
	migrateCmd.Flags().BoolVar(&restoreBackup, "restore", false, "Restore database from backup")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	if !migrateAll && migrateDatabase == "" {
		return fmt.Errorf("either --database or --all must be specified")
	}

	if migrateAll && migrateDatabase != "" {
		return fmt.Errorf("cannot specify both --database and --all")
	}

	// Create storage manager
	storage, err := db.NewStorageManager(generalRootDir)
	if err != nil {
		return fmt.Errorf("failed to create storage manager: %w", err)
	}
	defer storage.Close()

	migrator := db.NewMigrationManager(storage)

	// Handle restore
	if restoreBackup {
		if migrateDatabase == "" {
			return fmt.Errorf("--database is required when using --restore")
		}
		fmt.Printf("Restoring database '%s' from backup...\n", migrateDatabase)
		if err := migrator.RestoreBackup(migrateDatabase); err != nil {
			return fmt.Errorf("restore failed: %w", err)
		}
		fmt.Println("Restore complete!")
		return nil
	}

	// Handle verify-only
	if verifyOnly {
		if migrateDatabase == "" {
			return fmt.Errorf("--database is required when using --verify")
		}
		return migrator.VerifyMigration(migrateDatabase)
	}

	// Migrate all databases
	if migrateAll {
		if !skipBackup {
			fmt.Println("Warning: Creating backups for all databases...")
			// Get list of databases
			entries, err := os.ReadDir(generalRootDir)
			if err != nil {
				return fmt.Errorf("failed to read data directory: %w", err)
			}

			for _, entry := range entries {
				// Skip WAL files and non-directories
				if entry.IsDir() && !strings.HasPrefix(entry.Name(), db.WALFilePrefix) {
					if err := migrator.CreateBackup(entry.Name()); err != nil {
						fmt.Printf("Warning: backup failed for '%s': %v\n", entry.Name(), err)
					}
				}
			}
		}

		return migrator.MigrateAllDatabases()
	}

	// Migrate single database
	if !skipBackup {
		if err := migrator.CreateBackup(migrateDatabase); err != nil {
			return fmt.Errorf("backup failed: %w", err)
		}
	}

	if err := migrator.MigrateDatabase(migrateDatabase); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// Verify after migration
	if err := migrator.VerifyMigration(migrateDatabase); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	fmt.Println("\nMigration complete!")
	fmt.Printf("To restore from backup if needed: %s migrate --database %s --restore\n",
		os.Args[0], migrateDatabase)

	return nil
}

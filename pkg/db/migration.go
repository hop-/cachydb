package db

import (
	"fmt"
	"os"
	"path/filepath"
)

// MigrationManager handles migration from JSON to binary storage
type MigrationManager struct {
	storage *StorageManager
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(storage *StorageManager) *MigrationManager {
	return &MigrationManager{
		storage: storage,
	}
}

// MigrateDatabase migrates a single database from JSON to binary format
func (mm *MigrationManager) MigrateDatabase(dbName string) error {
	fmt.Printf("Migrating database '%s' from JSON to binary format...\n", dbName)

	// Load database with JSON format
	oldFormat := mm.storage.Format
	mm.storage.Format = FormatJSON
	db, err := mm.storage.LoadDatabase(dbName)
	if err != nil {
		mm.storage.Format = oldFormat
		return fmt.Errorf("failed to load database: %w", err)
	}

	// Switch to binary format and save
	mm.storage.Format = FormatBinary
	if err := mm.storage.SaveDatabase(db); err != nil {
		mm.storage.Format = oldFormat
		return fmt.Errorf("failed to save database in binary format: %w", err)
	}

	mm.storage.Format = oldFormat

	fmt.Printf("Database '%s' migrated successfully\n", dbName)
	return nil
}

// MigrateCollection migrates a single collection from JSON to binary format
func (mm *MigrationManager) MigrateCollection(dbName, collName string) error {
	fmt.Printf("Migrating collection '%s/%s' from JSON to binary format...\n", dbName, collName)

	// Load collection with JSON format
	oldFormat := mm.storage.Format
	mm.storage.Format = FormatJSON
	coll, err := mm.storage.LoadCollection(dbName, collName)
	if err != nil {
		mm.storage.Format = oldFormat
		return fmt.Errorf("failed to load collection: %w", err)
	}

	// Switch to binary format and save
	mm.storage.Format = FormatBinary
	if err := mm.storage.SaveCollection(dbName, coll); err != nil {
		mm.storage.Format = oldFormat
		return fmt.Errorf("failed to save collection in binary format: %w", err)
	}

	// Remove old JSON documents file
	docsPath := filepath.Join(mm.storage.RootDir, dbName, collName, "documents.json")
	if err := os.Remove(docsPath); err != nil && !os.IsNotExist(err) {
		fmt.Printf("Warning: failed to remove old JSON file: %v\n", err)
	}

	mm.storage.Format = oldFormat

	fmt.Printf("Collection '%s/%s' migrated successfully\n", dbName, collName)
	return nil
}

// MigrateAllDatabases migrates all databases from JSON to binary format
func (mm *MigrationManager) MigrateAllDatabases() error {
	fmt.Println("Starting migration of all databases from JSON to binary format...")

	// Find all database directories
	entries, err := os.ReadDir(mm.storage.RootDir)
	if err != nil {
		return fmt.Errorf("failed to read root directory: %w", err)
	}

	migratedCount := 0
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "wal" {
			continue
		}

		dbName := entry.Name()

		// Check if database is already in binary format by looking for collection.data files
		if mm.isDatabaseBinary(dbName) {
			fmt.Printf("Database '%s' is already in binary format, skipping\n", dbName)
			continue
		}

		if err := mm.MigrateDatabase(dbName); err != nil {
			return fmt.Errorf("failed to migrate database '%s': %w", dbName, err)
		}

		migratedCount++
	}

	fmt.Printf("\nMigration complete! Migrated %d database(s)\n", migratedCount)
	return nil
}

// isDatabaseBinary checks if a database is already using binary format
func (mm *MigrationManager) isDatabaseBinary(dbName string) bool {
	dbDir := filepath.Join(mm.storage.RootDir, dbName)

	// Read collections
	entries, err := os.ReadDir(dbDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check if collection.data exists (binary format)
		dataPath := filepath.Join(dbDir, entry.Name(), "collection.data")
		if _, err := os.Stat(dataPath); err == nil {
			return true
		}

		// Check if documents.json exists (JSON format)
		jsonPath := filepath.Join(dbDir, entry.Name(), "documents.json")
		if _, err := os.Stat(jsonPath); err == nil {
			return false
		}
	}

	// If no collections, consider it not binary (will be created as binary)
	return false
}

// VerifyMigration verifies that the migration was successful
func (mm *MigrationManager) VerifyMigration(dbName string) error {
	fmt.Printf("Verifying migration of database '%s'...\n", dbName)

	// Load database with binary format
	oldFormat := mm.storage.Format
	mm.storage.Format = FormatBinary
	db, err := mm.storage.LoadDatabase(dbName)
	if err != nil {
		mm.storage.Format = oldFormat
		return fmt.Errorf("failed to load database in binary format: %w", err)
	}
	mm.storage.Format = oldFormat

	// Count documents
	totalDocs := 0
	for _, coll := range db.Collections {
		docCount := len(coll.Documents)
		totalDocs += docCount
		fmt.Printf("  Collection '%s': %d documents\n", coll.Name, docCount)
	}

	fmt.Printf("Database '%s' verification complete: %d collections, %d total documents\n",
		dbName, len(db.Collections), totalDocs)
	return nil
}

// CreateBackup creates a backup of the database before migration
func (mm *MigrationManager) CreateBackup(dbName string) error {
	dbDir := filepath.Join(mm.storage.RootDir, dbName)
	backupDir := filepath.Join(mm.storage.RootDir, dbName+".backup")

	fmt.Printf("Creating backup of database '%s'...\n", dbName)

	// Check if backup already exists
	if _, err := os.Stat(backupDir); err == nil {
		return fmt.Errorf("backup directory already exists: %s", backupDir)
	}

	// Copy directory recursively
	if err := copyDir(dbDir, backupDir); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	fmt.Printf("Backup created at: %s\n", backupDir)
	return nil
}

// RestoreBackup restores a database from backup
func (mm *MigrationManager) RestoreBackup(dbName string) error {
	dbDir := filepath.Join(mm.storage.RootDir, dbName)
	backupDir := filepath.Join(mm.storage.RootDir, dbName+".backup")

	fmt.Printf("Restoring database '%s' from backup...\n", dbName)

	// Check if backup exists
	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		return fmt.Errorf("backup directory does not exist: %s", backupDir)
	}

	// Remove current database
	if err := os.RemoveAll(dbDir); err != nil {
		return fmt.Errorf("failed to remove current database: %w", err)
	}

	// Restore from backup
	if err := copyDir(backupDir, dbDir); err != nil {
		return fmt.Errorf("failed to restore from backup: %w", err)
	}

	fmt.Printf("Database '%s' restored from backup\n", dbName)
	return nil
}

// copyDir recursively copies a directory
func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

// copyFile copies a single file
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	return os.WriteFile(dst, data, 0644)
}

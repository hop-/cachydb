package db

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// CurrentSchemaVersion is the schema version new databases are created
// with. Open rejects databases with a newer version (ErrSchemaTooNew).
const CurrentSchemaVersion = 1

// MigrationFunc migrates the database dbName from one schema version to
// the next. It runs on an opened engine (after recovery) and must use the
// normal engine operations, which are WAL-logged. Because the version bump
// is written after the function returns, a crash in between reruns the
// migration on the next attempt, so migrations should be idempotent.
//
// Example:
//
//	func init() {
//		db.RegisterMigration(1, func(ctx context.Context, e *db.Engine, dbName string) error {
//			d, err := e.Database(dbName)
//			if err != nil {
//				return err
//			}
//			return d.CreateCollection(ctx, "audit", nil)
//		})
//	}
type MigrationFunc func(ctx context.Context, e *Engine, dbName string) error

// registry holds the registered migrations: from version -> function that
// migrates to from+1.
var registry = struct {
	mu         sync.RWMutex
	migrations map[int]MigrationFunc
}{migrations: make(map[int]MigrationFunc)}

// RegisterMigration registers fn as the migration from fromVersion to
// fromVersion+1, replacing any earlier registration for fromVersion.
func RegisterMigration(fromVersion int, fn MigrationFunc) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.migrations[fromVersion] = fn
}

// ListMigrations returns the registered from-versions in ascending order.
func ListMigrations() []int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return slices.Sorted(maps.Keys(registry.migrations))
}

func migrationFor(fromVersion int) (MigrationFunc, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	fn, ok := registry.migrations[fromVersion]
	return fn, ok
}

// MigrateDatabase applies the registered migrations to dbName one version
// at a time until it reaches targetVersion, persisting the new version
// after each step. Downgrades are rejected. Migrations are serialized per
// engine.
func (e *Engine) MigrateDatabase(ctx context.Context, dbName string, targetVersion int) error {
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()
	e.migrateMu.Lock()
	defer e.migrateMu.Unlock()
	return e.migrateDatabase(ctx, dbName, targetVersion)
}

func (e *Engine) migrateDatabase(ctx context.Context, dbName string, targetVersion int) error {
	d, err := e.Database(dbName)
	if err != nil {
		return err
	}
	current := d.SchemaVersion()
	if current > targetVersion {
		return fmt.Errorf("migrating database %q: cannot downgrade from version %d to %d", dbName, current, targetVersion)
	}
	for v := current; v < targetVersion; v++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		fn, ok := migrationFor(v)
		if !ok {
			return fmt.Errorf("migrating database %q: no migration registered from version %d to %d", dbName, v, v+1)
		}
		e.logger.Info("cachydb: migrating database", "database", dbName, "from", v, "to", v+1)
		if err := fn(ctx, e, dbName); err != nil {
			return fmt.Errorf("migrating database %q from version %d to %d: %w", dbName, v, v+1, err)
		}
		if err := d.setSchemaVersion(v + 1); err != nil {
			return fmt.Errorf("migrating database %q: %w", dbName, err)
		}
	}
	return nil
}

// MigrateAll migrates every database to targetVersion. Databases dropped
// while it runs are skipped.
func (e *Engine) MigrateAll(ctx context.Context, targetVersion int) error {
	if err := e.enter(); err != nil {
		return err
	}
	defer e.exit()
	e.migrateMu.Lock()
	defer e.migrateMu.Unlock()
	for _, name := range e.ListDatabases() {
		if err := e.migrateDatabase(ctx, name, targetVersion); err != nil {
			if errors.Is(err, ErrNotFound) && e.lookupDatabase(name) == nil {
				continue
			}
			return err
		}
	}
	return nil
}

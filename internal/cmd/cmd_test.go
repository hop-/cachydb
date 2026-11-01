package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/internal/config"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

func openEngine(t *testing.T, dir string) *db.Engine {
	t.Helper()
	e, err := db.Open(context.Background(), db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func closeEngine(t *testing.T, e *db.Engine) {
	t.Helper()
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// seed creates databases "main" (users: 2 documents, empty: 0) and "other"
// (no collections) in dir.
func seed(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	e := openEngine(t, dir)
	defer closeEngine(t, e)

	d, err := e.CreateDatabase(ctx, "main")
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	for _, name := range []string{"users", "empty"} {
		if err := d.CreateCollection(ctx, name, nil); err != nil {
			t.Fatalf("CreateCollection %s: %v", name, err)
		}
	}
	c, err := d.Collection("users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	for i := range 2 {
		if _, err := c.Insert(ctx, &db.Document{Data: map[string]any{"n": i}}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	if _, err := e.CreateDatabase(ctx, "other"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
}

func TestListDatabases(t *testing.T) {
	seeded := t.TempDir()
	seed(t, seeded)

	tests := []struct {
		name        string
		dir         string
		collections bool
		want        string
	}{
		{
			name: "empty data directory",
			dir:  t.TempDir(),
			want: "No databases found\n",
		},
		{
			name: "databases only",
			dir:  seeded,
			want: "Found 2 database(s):\n\n" +
				"  main (schema version: 1)\n" +
				"  other (schema version: 1)\n",
		},
		{
			name:        "with collections and counts",
			dir:         seeded,
			collections: true,
			want: "Found 2 database(s):\n\n" +
				"  main (schema version: 1)\n" +
				"    └─ empty (0 documents)\n" +
				"    └─ users (2 documents)\n" +
				"  other (schema version: 1)\n" +
				"    └─ (no collections)\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := listDatabases(context.Background(), &out, db.Options{Dir: tt.dir}, tt.collections); err != nil {
				t.Fatalf("listDatabases: %v", err)
			}
			if got := out.String(); got != tt.want {
				t.Fatalf("output:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

// TestUtilsFailWhileServerRuns holds the data-directory lock like a
// running server and checks that utils commands refuse to run.
func TestUtilsFailWhileServerRuns(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir)
	defer closeEngine(t, e)
	opts := db.Options{Dir: dir}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "list",
			run: func() error {
				return listDatabases(context.Background(), io.Discard, opts, true)
			},
		},
		{
			name: "migrate",
			run: func() error {
				return runMigration(context.Background(), io.Discard, io.Discard, opts,
					migrateParams{all: true, target: db.CurrentSchemaVersion})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if !errors.Is(err, db.ErrDataDirInUse) {
				t.Fatalf("err = %v, want ErrDataDirInUse", err)
			}
			if !strings.Contains(err.Error(), "data directory is in use by another cachydb process") {
				t.Fatalf("err = %q, want the data-directory-in-use message", err)
			}
		})
	}
}

func TestMigrateParamsValidate(t *testing.T) {
	tests := []struct {
		name    string
		p       migrateParams
		wantErr string
	}{
		{name: "list ignores other flags", p: migrateParams{list: true, all: true, database: "main", target: 99}},
		{name: "show version", p: migrateParams{showVersion: true, database: "main"}},
		{name: "show version needs database", p: migrateParams{showVersion: true}, wantErr: "--database is required"},
		{name: "single database", p: migrateParams{database: "main", target: db.CurrentSchemaVersion}},
		{name: "all", p: migrateParams{all: true, target: db.CurrentSchemaVersion}},
		{name: "neither database nor all", p: migrateParams{target: 1}, wantErr: "either --database or --all"},
		{name: "both database and all", p: migrateParams{all: true, database: "main", target: 1}, wantErr: "cannot specify both"},
		{name: "target below one", p: migrateParams{all: true, target: 0}, wantErr: "schema versions start at 1"},
		{name: "target above current without migrations", p: migrateParams{all: true, target: db.CurrentSchemaVersion + 1}, wantErr: "latest supported schema version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.p.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRunMigration(t *testing.T) {
	dir := t.TempDir()
	seed(t, dir)
	opts := db.Options{Dir: dir}

	tests := []struct {
		name    string
		p       migrateParams
		wantOut string
		wantErr error
	}{
		{
			name:    "show version",
			p:       migrateParams{showVersion: true, database: "main"},
			wantOut: "Database 'main' is at schema version 1\nCurrent schema version: 1\n",
		},
		{
			name:    "show version of missing database",
			p:       migrateParams{showVersion: true, database: "nope"},
			wantErr: db.ErrNotFound,
		},
		{
			name:    "migrate single database to current",
			p:       migrateParams{database: "main", target: db.CurrentSchemaVersion},
			wantOut: "Database 'main' migrated successfully!\n",
		},
		{
			name:    "migrate missing database",
			p:       migrateParams{database: "nope", target: db.CurrentSchemaVersion},
			wantErr: db.ErrNotFound,
		},
		{
			name:    "migrate all to current",
			p:       migrateParams{all: true, target: db.CurrentSchemaVersion},
			wantOut: "Migrating all databases to version 1...\nAll databases migrated successfully!\n",
		},
		{
			name:    "list without migrations",
			p:       migrateParams{list: true},
			wantOut: "No migrations registered yet\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			err := runMigration(context.Background(), &out, &errOut, opts, tt.p)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("runMigration: %v", err)
			}
			if got := out.String(); got != tt.wantOut {
				t.Fatalf("output:\n%s\nwant:\n%s", got, tt.wantOut)
			}
		})
	}
}

func TestEngineOptionsFromFlags(t *testing.T) {
	savedRoot, savedAge := generalRootDir, generalWALRetentionAge
	savedSize, savedCache := generalWALRetentionSize, generalCacheSize
	t.Cleanup(func() {
		generalRootDir, generalWALRetentionAge = savedRoot, savedAge
		generalWALRetentionSize, generalCacheSize = savedSize, savedCache
	})

	tests := []struct {
		name      string
		flags     map[string]string
		wantDir   string
		wantAge   time.Duration
		wantSize  int64
		wantCache int64
	}{
		{
			name:      "configured defaults",
			flags:     map[string]string{"root": "/data"},
			wantDir:   "/data",
			wantAge:   config.GetConfig().WALRetentionAge,
			wantSize:  int64(config.GetConfig().WALRetentionSize),
			wantCache: int64(config.GetConfig().CacheSize),
		},
		{
			name:      "explicit sizes",
			flags:     map[string]string{"root": "/data", "wal-retention-age": "2h", "wal-retention-size": "512MiB", "cache-size": "1GiB"},
			wantDir:   "/data",
			wantAge:   2 * time.Hour,
			wantSize:  512 << 20,
			wantCache: 1 << 30,
		},
		{
			name:      "zero disables",
			flags:     map[string]string{"root": "/data", "wal-retention-age": "0", "wal-retention-size": "0", "cache-size": "0"},
			wantDir:   "/data",
			wantAge:   -1,
			wantSize:  -1,
			wantCache: -1,
		},
		{
			name:      "empty root falls back to the configured directory",
			flags:     map[string]string{"root": ""},
			wantDir:   config.GetConfig().RootDir,
			wantAge:   config.GetConfig().WALRetentionAge,
			wantSize:  int64(config.GetConfig().WALRetentionSize),
			wantCache: int64(config.GetConfig().CacheSize),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := config.GetConfig()
			generalWALRetentionAge, generalWALRetentionSize, generalCacheSize = c.WALRetentionAge, c.WALRetentionSize, c.CacheSize
			fs := utilsCmd.PersistentFlags()
			for name, value := range tt.flags {
				if err := fs.Set(name, value); err != nil {
					t.Fatalf("setting --%s=%s: %v", name, value, err)
				}
			}

			opts := engineOptions()
			if opts.Dir != tt.wantDir {
				t.Errorf("Dir = %q, want %q", opts.Dir, tt.wantDir)
			}
			if opts.WALRetentionAge != tt.wantAge {
				t.Errorf("WALRetentionAge = %v, want %v", opts.WALRetentionAge, tt.wantAge)
			}
			if opts.WALRetentionSize != tt.wantSize {
				t.Errorf("WALRetentionSize = %d, want %d", opts.WALRetentionSize, tt.wantSize)
			}
			if opts.CacheSize != tt.wantCache {
				t.Errorf("CacheSize = %d, want %d", opts.CacheSize, tt.wantCache)
			}
		})
	}
}

func TestEngineFlagsOnCommands(t *testing.T) {
	tests := []struct {
		name   string
		lookup func(name string) bool
	}{
		{name: "root", lookup: func(n string) bool { return rootCmd.Flags().Lookup(n) != nil }},
		{name: "app", lookup: func(n string) bool { return appCmd.Flags().Lookup(n) != nil }},
		{name: "utils", lookup: func(n string) bool { return utilsCmd.PersistentFlags().Lookup(n) != nil }},
		{name: "utils list", lookup: func(n string) bool {
			return listCmd.Flags().Lookup(n) != nil || listCmd.InheritedFlags().Lookup(n) != nil
		}},
		{name: "utils migrate", lookup: func(n string) bool { return migrateCmd.InheritedFlags().Lookup(n) != nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, flag := range []string{"wal-retention-age", "wal-retention-size", "cache-size"} {
				if !tt.lookup(flag) {
					t.Errorf("--%s missing", flag)
				}
			}
		})
	}
}

// TestServerSettingsPrecedence checks flag > env > default for the server
// settings: the environment is loaded with config.Init, the flags get their
// defaults from it, and an explicit flag wins.
func TestServerSettingsPrecedence(t *testing.T) {
	envKeys := []string{"ROOT_DIR", "DB_NAME", "TRANSPORT", "PORT"}
	savedRoot, savedDB := generalRootDir, generalDBName
	savedTransport, savedPort := generalTransport, generalServerPort
	t.Cleanup(func() {
		generalRootDir, generalDBName = savedRoot, savedDB
		generalTransport, generalServerPort = savedTransport, savedPort
	})

	type want struct {
		root, db, transport string
		port                int
	}
	defaults := func(t *testing.T) want {
		t.Helper()
		home, err := os.UserHomeDir()
		if err != nil {
			home = "."
		}
		return want{root: filepath.Join(home, config.GetConfig().RootDirName), db: "main", transport: "stdio", port: 7601}
	}

	tests := []struct {
		name string
		env  map[string]string
		args []string
		want func(t *testing.T) want
	}{
		{name: "defaults", want: defaults},
		{
			name: "env only",
			env:  map[string]string{"ROOT_DIR": "/env/root", "DB_NAME": "envdb", "TRANSPORT": "http", "PORT": "8000"},
			want: func(*testing.T) want { return want{root: "/env/root", db: "envdb", transport: "http", port: 8000} },
		},
		{
			name: "flag only",
			args: []string{"-R", "/flag/root", "-d", "flagdb", "-t", "http", "-p", "9000"},
			want: func(*testing.T) want { return want{root: "/flag/root", db: "flagdb", transport: "http", port: 9000} },
		},
		{
			name: "flag over env",
			env:  map[string]string{"ROOT_DIR": "/env/root", "DB_NAME": "envdb", "TRANSPORT": "http", "PORT": "8000"},
			args: []string{"--root", "/flag/root", "--db", "flagdb", "--transport", "stdio", "--port", "9000"},
			want: func(*testing.T) want { return want{root: "/flag/root", db: "flagdb", transport: "stdio", port: 9000} },
		},
		{
			name: "env and flag mixed",
			env:  map[string]string{"ROOT_DIR": "/env/root", "TRANSPORT": "http"},
			args: []string{"-d", "flagdb"},
			want: func(t *testing.T) want {
				w := defaults(t)
				w.root, w.db, w.transport = "/env/root", "flagdb", "http"
				return w
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Registered before t.Setenv, so it runs after the environment
			// is restored and reloads the original configuration.
			t.Cleanup(func() {
				if err := config.Init(); err != nil {
					t.Errorf("restoring config: %v", err)
				}
			})
			for _, k := range envKeys {
				t.Setenv(k, tt.env[k])
				if _, ok := tt.env[k]; !ok {
					if err := os.Unsetenv(k); err != nil {
						t.Fatalf("unsetenv %s: %v", k, err)
					}
				}
			}
			if err := config.Init(); err != nil {
				t.Fatalf("config.Init: %v", err)
			}

			cmd := &cobra.Command{Use: "test", RunE: func(*cobra.Command, []string) error { return nil }}
			setAllFlagsToCmd(cmd)
			cmd.SetArgs(tt.args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute: %v", err)
			}

			w := tt.want(t)
			if got := rootDir(); got != w.root {
				t.Errorf("root = %q, want %q", got, w.root)
			}
			if got := dbName(); got != w.db {
				t.Errorf("db = %q, want %q", got, w.db)
			}
			if generalTransport != w.transport {
				t.Errorf("transport = %q, want %q", generalTransport, w.transport)
			}
			if generalServerPort != w.port {
				t.Errorf("port = %d, want %d", generalServerPort, w.port)
			}
		})
	}
}

func TestValidateTransport(t *testing.T) {
	tests := []struct {
		transport string
		wantErr   bool
	}{
		{transport: "stdio"},
		{transport: "http"},
		{transport: "", wantErr: true},
		{transport: "HTTP", wantErr: true},
		{transport: "sse", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.transport, func(t *testing.T) {
			err := validateTransport(tt.transport)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateTransport(%q) = %v, wantErr %v", tt.transport, err, tt.wantErr)
			}
		})
	}
}

package cmd

import (
	"context"
	"errors"

	"github.com/hop-/cachydb/internal/app"
	"github.com/hop-/cachydb/internal/config"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// utilsCmd represents the utils command group
var utilsCmd = &cobra.Command{
	Use:   "utils",
	Short: "Utility commands for database maintenance and management",
	Long: `Utility commands for database maintenance and management.
This includes migration, backup, repair, and other administrative tasks.

Utility commands open the database engine and take the data-directory
lock, so they fail while a cachydb server is running on the same directory.`,
}

func init() {
	// Add root directory flag to utils command (inherited by all subcommands)
	utilsCmd.PersistentFlags().StringVarP(
		&generalRootDir,
		"root", "R",
		config.GetConfig().RootDir,
		"root directory for application data and configurations",
	)
	setEngineFlags(utilsCmd.PersistentFlags())

	rootCmd.AddCommand(utilsCmd)
}

// setEngineFlags adds the engine tuning flags (defaults from the
// configuration) to fs.
func setEngineFlags(fs *pflag.FlagSet) {
	c := config.GetConfig()
	generalWALRetentionAge = c.WALRetentionAge
	generalWALRetentionSize = c.WALRetentionSize
	generalCacheSize = c.CacheSize

	fs.DurationVar(
		&generalWALRetentionAge,
		"wal-retention-age",
		c.WALRetentionAge,
		"keep processed WAL segments for this long, e.g. 24h (0 = no age limit)",
	)
	fs.Var(
		&generalWALRetentionSize,
		"wal-retention-size",
		"keep at most this much processed WAL, e.g. 1GiB (0 = no size limit)",
	)
	fs.Var(
		&generalCacheSize,
		"cache-size",
		"document cache size, e.g. 64MiB (0 = cache disabled)",
	)
}

// rootDir returns the data directory from the flags, falling back to the
// configured default at runtime.
func rootDir() string {
	if generalRootDir != "" {
		return generalRootDir
	}
	return config.GetConfig().RootDir
}

// newBuilder returns an app builder with the data directory and engine
// settings from the flags.
func newBuilder() *app.Builder {
	return app.NewBuilder().
		WithRootDir(rootDir()).
		WithWALRetention(generalWALRetentionAge, int64(generalWALRetentionSize)).
		WithCacheSize(int64(generalCacheSize))
}

// engineOptions returns the engine options utility commands open the
// engine with.
func engineOptions() db.Options {
	return newBuilder().EngineOptions()
}

// withEngine opens the engine (taking the data-directory lock and running
// recovery), calls fn, and closes the engine, bounded by
// app.ShutdownTimeout.
func withEngine(ctx context.Context, opts db.Options, fn func(*db.Engine) error) (err error) {
	engine, err := db.Open(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), app.ShutdownTimeout)
		defer cancel()
		if cerr := engine.Close(closeCtx); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	return fn(engine)
}

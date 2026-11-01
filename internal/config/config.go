package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config is the environment configuration. Flags override every value
// (precedence: flag > environment variable > default).
type Config struct {
	// Port is the HTTP transport port (env PORT).
	Port int `envconfig:"PORT" default:"7601"`
	// RootDir is the data directory (env ROOT_DIR); empty means
	// $HOME/<RootDirName>.
	RootDir     string `envconfig:"ROOT_DIR" default:""`
	RootDirName string `default:".cachydb"`
	// DBName is the default database the server creates and uses (env
	// DB_NAME).
	DBName string `envconfig:"DB_NAME" default:"main"`
	// Transport is "stdio" or "http" (env TRANSPORT).
	Transport string `envconfig:"TRANSPORT" default:"stdio"`

	// WALRetentionAge keeps processed WAL segments for this long; 0 means
	// no age limit.
	WALRetentionAge time.Duration `envconfig:"WAL_RETENTION_AGE" default:"24h"`
	// WALRetentionSize caps the total size of processed WAL segments; 0
	// means no size limit.
	WALRetentionSize ByteSize `envconfig:"WAL_RETENTION_SIZE" default:"1GiB"`
	// CacheSize bounds the document cache; 0 disables it.
	CacheSize ByteSize `envconfig:"CACHE_SIZE" default:"64MiB"`
}

var cfg Config
var (
	// Windows specific
	windowsRootDirName string
)

// Init loads the configuration from the environment and applies the
// defaults. It returns an error for a malformed value (for example
// WAL_RETENTION_AGE=soon); the fields that did parse are still set.
func Init() error {
	// Start from zero values: envconfig leaves a field without a default
	// untouched when its variable is unset, so a previous Init's value
	// would otherwise survive.
	cfg = Config{}
	err := envconfig.Process("", &cfg)
	if err != nil {
		err = fmt.Errorf("loading configuration from the environment: %w", err)
	}

	// An empty variable (DB_NAME=) means "not set", like an unset one.
	if cfg.DBName == "" {
		cfg.DBName = "main"
	}
	if cfg.Transport == "" {
		cfg.Transport = "stdio"
	}

	if windowsRootDirName != "" {
		cfg.RootDirName = windowsRootDirName
	}

	if cfg.RootDir == "" {
		homeDir, herr := os.UserHomeDir()
		if herr != nil {
			homeDir = "."
		}

		cfg.RootDir = filepath.Join(homeDir, cfg.RootDirName)
	}

	return err
}

// GetConfig returns the configuration loaded by Init.
func GetConfig() Config {
	return cfg
}

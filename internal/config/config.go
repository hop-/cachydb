package config

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Port        int    `env:"PORT" default:"7601"`
	RootDir     string `env:"ROOT_DIR" default:""`
	RootDirName string `default:".cachydb"`
	DBName      string `env:"DB_NAME" default:"main"`
	Transport   string `env:"TRANSPORT" default:"stdio"`

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
	err := envconfig.Process("", &cfg)
	if err != nil {
		err = fmt.Errorf("loading configuration from the environment: %w", err)
	}

	if windowsRootDirName != "" {
		cfg.RootDirName = windowsRootDirName
	}

	if cfg.RootDir == "" {
		homeDir, herr := os.UserHomeDir()
		if herr != nil {
			homeDir = "."
		}

		cfg.RootDir = path.Join(homeDir, cfg.RootDirName)
	}

	return err
}

func GetConfig() Config {
	return cfg
}

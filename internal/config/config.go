package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Config is the environment configuration. Flags override every value
// (precedence: flag > environment variable > default).
type Config struct {
	// Port is the HTTP transport port (env PORT).
	Port int
	// RootDir is the data directory (env ROOT_DIR); empty means
	// $HOME/<RootDirName>.
	RootDir     string
	RootDirName string
	// DBName is the default database the server creates and uses (env
	// DB_NAME).
	DBName string
	// Transport is "stdio" or "http" (env TRANSPORT).
	Transport string

	// WALRetentionAge keeps processed WAL segments for this long; 0 means
	// no age limit (env WAL_RETENTION_AGE).
	WALRetentionAge time.Duration
	// WALRetentionSize caps the total size of processed WAL segments; 0
	// means no size limit (env WAL_RETENTION_SIZE).
	WALRetentionSize ByteSize
	// CacheSize bounds the document cache; 0 disables it (env CACHE_SIZE).
	CacheSize ByteSize
}

// defaults returns the configuration of an empty environment. RootDir is
// derived from RootDirName by Init.
func defaults() Config {
	return Config{
		Port:             7601,
		RootDirName:      ".cachydb",
		DBName:           "main",
		Transport:        "stdio",
		WALRetentionAge:  24 * time.Hour,
		WALRetentionSize: GiB,
		CacheSize:        64 * MiB,
	}
}

var cfg Config
var (
	// Windows specific
	windowsRootDirName string
)

// Init loads the configuration from the environment and applies the
// defaults. It returns an error that names the variable for a malformed
// value (for example WAL_RETENTION_AGE=soon); that setting keeps its
// default and the others are still set.
func Init() error {
	cfg = defaults()
	if windowsRootDirName != "" {
		cfg.RootDirName = windowsRootDirName
	}

	err := errors.Join(
		fromEnv("PORT", &cfg.Port, strconv.Atoi),
		fromEnv("ROOT_DIR", &cfg.RootDir, asIs),
		fromEnv("DB_NAME", &cfg.DBName, asIs),
		fromEnv("TRANSPORT", &cfg.Transport, asIs),
		fromEnv("WAL_RETENTION_AGE", &cfg.WALRetentionAge, time.ParseDuration),
		fromEnv("WAL_RETENTION_SIZE", &cfg.WALRetentionSize, ParseByteSize),
		fromEnv("CACHE_SIZE", &cfg.CacheSize, ParseByteSize),
	)
	if err != nil {
		err = fmt.Errorf("loading configuration from the environment: %w", err)
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

// fromEnv sets *dst to the parsed value of the environment variable name.
// An empty variable (DB_NAME=) means "not set", like an unset one, and
// leaves *dst alone; so does a malformed value, which is returned as an
// error that names the variable.
func fromEnv[T any](name string, dst *T, parse func(string) (T, error)) error {
	s := os.Getenv(name)
	if s == "" {
		return nil
	}
	v, err := parse(s)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*dst = v
	return nil
}

// asIs is the parser of a string setting.
func asIs(s string) (string, error) { return s, nil }

// GetConfig returns the configuration loaded by Init.
func GetConfig() Config {
	return cfg
}

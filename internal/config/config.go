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
	// Host is the HTTP listen host (env HOST): 127.0.0.1 serves this
	// machine only, 0.0.0.0 every interface.
	Host string
	// Auth is the authentication mode, "auto", "required" or "off" (env
	// AUTH); internal/auth.ParseMode validates it.
	Auth string
	// TLSCert and TLSKey are the PEM certificate and key files of the
	// HTTP transport (env TLS_CERT, TLS_KEY); both or neither.
	TLSCert string
	TLSKey  string
	// BootstrapAdminToken (env BOOTSTRAP_ADMIN_TOKEN) or the file named by
	// BootstrapAdminTokenFile (env BOOTSTRAP_ADMIN_TOKEN_FILE) holds the
	// token the server admin "admin" gets at start-up. A secret: never
	// logged or printed. There is no flag, so it never shows in a process
	// listing.
	BootstrapAdminToken     string
	BootstrapAdminTokenFile string
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

	// DefaultLimit is the number of documents find_documents returns at
	// most when the call gives no limit (env DEFAULT_LIMIT); positive.
	DefaultLimit int
	// MaxResultBytes caps the JSON size of the documents one
	// find_documents call returns (env MAX_RESULT_BYTES); positive.
	MaxResultBytes ByteSize
}

// defaults returns the configuration of an empty environment. RootDir is
// derived from RootDirName by Init.
func defaults() Config {
	return Config{
		Port:             7601,
		Host:             "127.0.0.1",
		Auth:             "auto",
		RootDirName:      ".cachydb",
		DBName:           "main",
		Transport:        "stdio",
		WALRetentionAge:  24 * time.Hour,
		WALRetentionSize: GiB,
		CacheSize:        64 * MiB,
		DefaultLimit:     100,
		MaxResultBytes:   MiB,
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
		fromEnv("HOST", &cfg.Host, asIs),
		fromEnv("AUTH", &cfg.Auth, asIs),
		fromEnv("TLS_CERT", &cfg.TLSCert, asIs),
		fromEnv("TLS_KEY", &cfg.TLSKey, asIs),
		fromEnv("BOOTSTRAP_ADMIN_TOKEN", &cfg.BootstrapAdminToken, asIs),
		fromEnv("BOOTSTRAP_ADMIN_TOKEN_FILE", &cfg.BootstrapAdminTokenFile, asIs),
		fromEnv("ROOT_DIR", &cfg.RootDir, asIs),
		fromEnv("DB_NAME", &cfg.DBName, asIs),
		fromEnv("TRANSPORT", &cfg.Transport, asIs),
		fromEnv("WAL_RETENTION_AGE", &cfg.WALRetentionAge, time.ParseDuration),
		fromEnv("WAL_RETENTION_SIZE", &cfg.WALRetentionSize, ParseByteSize),
		fromEnv("CACHE_SIZE", &cfg.CacheSize, ParseByteSize),
		fromEnv("DEFAULT_LIMIT", &cfg.DefaultLimit, ParsePositiveInt),
		fromEnv("MAX_RESULT_BYTES", &cfg.MaxResultBytes, ParsePositiveByteSize),
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

// ParsePositiveInt parses a decimal integer greater than zero.
func ParsePositiveInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("%d: must be greater than 0", n)
	}
	return n, nil
}

// ParsePositiveByteSize parses a size (see ParseByteSize) greater than
// zero.
func ParsePositiveByteSize(s string) (ByteSize, error) {
	n, err := ParseByteSize(s)
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("invalid size %q: must be greater than 0", s)
	}
	return n, nil
}

// asIs is the parser of a string setting.
func asIs(s string) (string, error) { return s, nil }

// GetConfig returns the configuration loaded by Init.
func GetConfig() Config {
	return cfg
}

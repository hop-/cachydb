package config

import (
	"os"
	"path"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Port        int    `env:"PORT" default:"7601"`
	RootDir     string `env:"ROOT_DIR" default:""`
	RootDirName string `default:".cachydb"`
	DBName      string `env:"DB_NAME" default:"main"`
}

var cfg Config
var (
	// Windows specific
	windowsRootDirName string
)

func Init() {
	envconfig.Process("", &cfg)

	if windowsRootDirName != "" {
		cfg.RootDirName = windowsRootDirName
	}

	if cfg.RootDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			homeDir = "."
		}

		cfg.RootDir = path.Join(homeDir, cfg.RootDirName)
	}

}

func GetConfig() Config {
	return cfg
}

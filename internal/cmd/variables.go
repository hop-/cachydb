package cmd

import (
	"time"

	"github.com/hop-/cachydb/internal/config"
)

var (
	Version        = "" // This will be set during build time using -ldflags "-X github.com/hop-/cachydb/internal/cmd.Version=$(git describe --tags --always)"
	defaultVersion = "v0.0.0-dev"

	// configErr holds the result of loading the configuration. It is a
	// package-level variable so the configuration is loaded before any
	// init function reads it for flag defaults; Execute reports it.
	configErr = config.Init()

	generalRootDir          string
	generalServerPort       int
	generalTransport        string
	generalDBName           string
	generalWALRetentionAge  time.Duration
	generalWALRetentionSize config.ByteSize
	generalCacheSize        config.ByteSize
)

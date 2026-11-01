package db

import "errors"

// Sentinel errors returned (usually wrapped) by the engine. Callers inspect
// them with errors.Is.
var (
	ErrNotFound            = errors.New("not found")
	ErrExists              = errors.New("already exists")
	ErrInvalidName         = errors.New("invalid name")
	ErrSchemaValidation    = errors.New("schema validation failed")
	ErrClosed              = errors.New("engine closed")
	ErrDataDirInUse        = errors.New("data directory is in use by another cachydb process")
	ErrIncompatibleDataDir = errors.New("incompatible data directory")
	ErrWALCorrupt          = errors.New("WAL corrupt")
	ErrWALFailed           = errors.New("WAL failed; restart required")
	ErrSchemaTooNew        = errors.New("database schema version is newer than supported")
)

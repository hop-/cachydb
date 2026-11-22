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
	// ErrConditionFailed reports a conditional write (an "if" condition)
	// whose document no longer matches, or no longer exists; nothing was
	// written. Re-read the document and retry.
	ErrConditionFailed = errors.New("condition failed")
	// ErrUniqueViolation reports a write whose value for a unique index
	// already belongs to another document, or a unique index that cannot be
	// created because existing documents share a value; nothing was written.
	ErrUniqueViolation = errors.New("unique index violation")
	// ErrBatchTooLarge reports a Collection.Batch with more operations than
	// Options.MaxBatchDocs or a larger payload than Options.MaxBatchBytes;
	// the error states the limit. Nothing was written. Split the batch.
	ErrBatchTooLarge = errors.New("batch too large")
	// ErrInvalidUpdate reports a misuse of the update operators of
	// UpdateSpec (Inc, Push, AddToSet, Pull): an operator on a field of the
	// wrong type (inc on a non-number, push/add_to_set/pull on a
	// non-array), a non-numeric inc value, or a field named by more than one
	// of Set, Unset, and the operators. Nothing was written.
	ErrInvalidUpdate = errors.New("invalid update")
)

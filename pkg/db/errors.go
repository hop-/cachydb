package db

import (
	"errors"
	"fmt"
)

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
	// ErrInvalidArgument reports a query or argument the engine cannot
	// take: an unknown filter operator, exists with a non-boolean or
	// starts_with with a non-string value, a bad sort key, an empty
	// Distinct field, a document id that is empty or too long, a write
	// query of UpdateMany or DeleteMany with sort, fields, skip, or limit,
	// or a malformed batch operation. Nothing was read or written.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrQueryInterrupted reports a query that compactions or index
	// changes made start over too often; nothing is wrong with the query
	// or the data. Retry it.
	ErrQueryInterrupted = errors.New("query interrupted")
)

// sentinelError is an error whose text is err's alone and that also
// matches sentinel with errors.Is, so a sentinel can be added to an
// existing error without changing its message.
type sentinelError struct {
	sentinel error
	err      error
}

func (e *sentinelError) Error() string   { return e.err.Error() }
func (e *sentinelError) Unwrap() []error { return []error{e.sentinel, e.err} }

// withSentinel returns err, unchanged in text, also matching sentinel.
func withSentinel(sentinel, err error) error {
	return &sentinelError{sentinel: sentinel, err: err}
}

// invalidArgf is fmt.Errorf that also matches ErrInvalidArgument.
func invalidArgf(format string, args ...any) error {
	return withSentinel(ErrInvalidArgument, fmt.Errorf(format, args...))
}

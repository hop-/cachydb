package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/pkg/db"
)

// Stable error codes (approach-agent-interface §6). Every tool error text
// starts with one of them in brackets, then the action, the cause, and
// any hint: "[not_found] find documents: collection ...". Argument
// validation errors the SDK raises before a handler runs keep the SDK's
// text ("validating \"arguments\": ...") and carry no code.
const (
	CodeNotFound         = "not_found"
	CodeAlreadyExists    = "already_exists"
	CodeInvalidName      = "invalid_name"
	CodeInvalidArgument  = "invalid_argument"
	CodeSchemaViolation  = "schema_violation"
	CodeUniqueViolation  = "unique_violation"
	CodeConditionFailed  = "condition_failed"
	CodeLimitExceeded    = "limit_exceeded"
	CodePermissionDenied = "permission_denied"
	CodeUnavailable      = "unavailable"
	CodeCancelled        = "cancelled"
	CodeInternal         = "internal"
)

// ErrorCodes lists every error code a tool error can start with, in the
// order of the errors docs topic.
var ErrorCodes = []string{
	CodeNotFound, CodeAlreadyExists, CodeInvalidName, CodeInvalidArgument,
	CodeSchemaViolation, CodeUniqueViolation, CodeConditionFailed,
	CodeLimitExceeded, CodePermissionDenied, CodeUnavailable, CodeCancelled,
	CodeInternal,
}

// ErrorCodeOf returns the code a tool error text starts with, and whether
// it is one of ErrorCodes.
func ErrorCodeOf(text string) (string, bool) {
	rest, ok := strings.CutPrefix(text, "[")
	if !ok {
		return "", false
	}
	code, _, ok := strings.Cut(rest, "] ")
	if !ok {
		return "", false
	}
	for _, c := range ErrorCodes {
		if c == code {
			return code, true
		}
	}
	return code, false
}

// errorCode maps an error to its code. The order follows toolError's
// hints, so the code and the hint of an error agree.
func errorCode(err error) string {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return CodeNotFound
	case errors.Is(err, db.ErrSchemaValidation):
		return CodeSchemaViolation
	case errors.Is(err, db.ErrInvalidUpdate):
		return CodeInvalidArgument
	case errors.Is(err, db.ErrConditionFailed):
		return CodeConditionFailed
	case errors.Is(err, auth.ErrPermissionDenied), errors.Is(err, auth.ErrUnauthenticated):
		return CodePermissionDenied
	case errors.Is(err, db.ErrExists):
		return CodeAlreadyExists
	case errors.Is(err, db.ErrInvalidName):
		return CodeInvalidName
	case errors.Is(err, db.ErrClosed), errors.Is(err, db.ErrWALFailed), errors.Is(err, db.ErrWALCorrupt),
		errors.Is(err, db.ErrQueryInterrupted):
		return CodeUnavailable
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return CodeCancelled
	case errors.Is(err, db.ErrUniqueViolation):
		return CodeUniqueViolation
	case errors.Is(err, db.ErrBatchTooLarge):
		return CodeLimitExceeded
	case errors.Is(err, db.ErrInvalidArgument), errors.Is(err, auth.ErrInvalidLevel),
		errors.Is(err, auth.ErrLastServerAdmin):
		return CodeInvalidArgument
	default:
		return CodeInternal
	}
}

// codedError is a tool error: "[code] " followed by the described error,
// which it wraps, so errors.Is still finds the engine's sentinel.
type codedError struct {
	code string
	err  error
}

func (e *codedError) Error() string { return "[" + e.code + "] " + e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }

// withCode prefixes described, the action-and-hint form of cause, with
// the code of cause.
func withCode(cause, described error) error {
	return &codedError{code: errorCode(cause), err: described}
}

// invalidArgf returns an argument error a handler found itself; it maps
// to invalid_argument and keeps the formatted text.
func invalidArgf(format string, args ...any) error {
	return &argError{msg: fmt.Sprintf(format, args...)}
}

// argError is an argument error raised by a handler (invalidArgf).
type argError struct{ msg string }

func (e *argError) Error() string { return e.msg }
func (e *argError) Unwrap() error { return db.ErrInvalidArgument }

package db

import "fmt"

// maxNameLen is the maximum length of a database, collection, or index name.
const maxNameLen = 64

// namePattern documents the rule ValidateName enforces.
const namePattern = `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`

// ValidateName reports whether name is a valid database, collection, or
// index name: it must match ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$. Names never
// contain '.', '/' or '\', so they cannot traverse out of the data
// directory or collide with engine files such as cachydb.lock.
//
// kind is "database", "collection", or "index" and is used only in the
// error message. The returned error wraps ErrInvalidName.
func ValidateName(kind, name string) error {
	if !validName(name) {
		return fmt.Errorf("%w: %s name %q must match %s", ErrInvalidName, kind, name, namePattern)
	}
	return nil
}

func validName(name string) bool {
	if len(name) == 0 || len(name) > maxNameLen {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case (c == '_' || c == '-') && i > 0:
		default:
			return false
		}
	}
	return true
}

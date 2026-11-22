package auth

import (
	"errors"
	"fmt"
	"strings"
)

// Mode is the authentication setting (AUTH, --auth): it decides, together
// with the transport and the listen host, whether clients need a token.
type Mode string

const (
	// ModeAuto requires tokens for HTTP on a host other machines can
	// reach, and nowhere else. It is the default.
	ModeAuto Mode = "auto"
	// ModeRequired requires tokens for every HTTP listener, loopback
	// included.
	ModeRequired Mode = "required"
	// ModeOff never requires tokens.
	ModeOff Mode = "off"
)

// ErrInvalidMode reports an authentication mode other than auto,
// required, or off.
var ErrInvalidMode = errors.New("invalid authentication mode")

// ParseMode returns the mode named s. The empty string is ModeAuto; any
// value other than auto, required, or off is an error wrapping
// ErrInvalidMode.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(s); m {
	case "":
		return ModeAuto, nil
	case ModeAuto, ModeRequired, ModeOff:
		return m, nil
	default:
		return "", fmt.Errorf("%w %q: use auto, required, or off", ErrInvalidMode, s)
	}
}

// IsLoopback reports whether host only accepts connections from this
// machine: 127.0.0.1, ::1 (with or without brackets), or localhost.
func IsLoopback(host string) bool {
	switch strings.ToLower(host) {
	case "127.0.0.1", "::1", "[::1]", "localhost":
		return true
	default:
		return false
	}
}

// Decision is what the start-up rules say about one server.
type Decision struct {
	// TokensRequired means every HTTP request to /mcp must carry the
	// token of an existing user, so the server must not start without
	// one.
	TokensRequired bool
	// OpenNetwork means HTTP on a non-loopback host with authentication
	// off: the server starts, and every client that reaches it has full
	// access, so the start-up logs a warning.
	OpenNetwork bool
}

// Decide applies the start-up rules to a server with authentication mode
// mode, serving HTTP on host if http is true and stdio otherwise:
//
//	transport  listener        auto             required         off
//	stdio      -               no tokens        no tokens        no tokens
//	HTTP       loopback host   no tokens        tokens required  no tokens
//	HTTP       any other host  tokens required  tokens required  no tokens, OpenNetwork
//
// stdio never needs tokens: whoever started the process owns the data
// directory. mode must be a valid Mode (see ParseMode); an unknown mode is
// treated as required, the safe side.
func Decide(mode Mode, http bool, host string) Decision {
	if !http {
		return Decision{}
	}
	loopback := IsLoopback(host)
	switch mode {
	case ModeOff:
		return Decision{OpenNetwork: !loopback}
	case ModeAuto:
		return Decision{TokensRequired: !loopback}
	default:
		return Decision{TokensRequired: true}
	}
}

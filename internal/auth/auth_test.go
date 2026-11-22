package auth

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestAuthLevels(t *testing.T) {
	if !(LevelNone < LevelRead && LevelRead < LevelWrite && LevelWrite < LevelAdmin && LevelAdmin < LevelServer) {
		t.Fatal("levels are not ordered none < read < write < admin < server")
	}
	for l, want := range map[Level]string{
		LevelNone: "none", LevelRead: "read", LevelWrite: "write", LevelAdmin: "admin", LevelServer: "server",
		Level(99): "Level(99)", Level(-1): "Level(-1)",
	} {
		if got := l.String(); got != want {
			t.Errorf("Level(%d).String() = %q, want %q", int(l), got, want)
		}
	}
	for l, want := range map[Level]bool{LevelNone: false, LevelRead: true, LevelWrite: true, LevelAdmin: true, LevelServer: false} {
		if got := l.grantable(); got != want {
			t.Errorf("%s.grantable() = %v, want %v", l, got, want)
		}
	}
}

func TestAuthParseLevel(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want Level
		ok   bool
	}{
		{"read", LevelRead, true},
		{"write", LevelWrite, true},
		{"admin", LevelAdmin, true},
		{"none", 0, false},
		{"server", 0, false},
		{"READ", 0, false},
		{"", 0, false},
	} {
		got, err := ParseLevel(tt.in)
		if tt.ok {
			if err != nil || got != tt.want {
				t.Errorf("ParseLevel(%q) = %v, %v; want %v", tt.in, got, err, tt.want)
			}
			continue
		}
		if !errors.Is(err, ErrInvalidLevel) {
			t.Errorf("ParseLevel(%q) err = %v, want ErrInvalidLevel", tt.in, err)
		}
	}
}

func TestAuthPrincipalCan(t *testing.T) {
	writer := Principal{Name: "w", Grants: map[string]Level{"shop": LevelWrite, "logs": LevelRead}}
	wildcard := Principal{Name: "r", Grants: map[string]Level{AllDatabases: LevelRead, "shop": LevelAdmin}}
	admin := Principal{Name: "a", ServerAdmin: true}
	nobody := Principal{Name: "n"}
	tests := []struct {
		name string
		p    Principal
		db   string
		need Level
		want bool
	}{
		{"none needs nothing", nobody, "shop", LevelNone, true},
		{"no grant", nobody, "shop", LevelRead, false},
		{"read below write grant", writer, "shop", LevelRead, true},
		{"write at write grant", writer, "shop", LevelWrite, true},
		{"admin above write grant", writer, "shop", LevelAdmin, false},
		{"read grant denies write", writer, "logs", LevelWrite, false},
		{"other database", writer, "other", LevelRead, false},
		{"wildcard covers any database", wildcard, "future", LevelRead, true},
		{"wildcard level caps", wildcard, "future", LevelWrite, false},
		{"higher of grant and wildcard", wildcard, "shop", LevelAdmin, true},
		{"server level needs a server admin", writer, "", LevelServer, false},
		{"server admin may everything", admin, "any", LevelAdmin, true},
		{"server admin has server level", admin, "", LevelServer, true},
		{"implicit full access", FullAccess(), "x", LevelServer, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Can(tt.db, tt.need); got != tt.want {
				t.Fatalf("Can(%q, %s) = %v, want %v", tt.db, tt.need, got, tt.want)
			}
		})
	}
	if fa := FullAccess(); !fa.Implicit || !fa.ServerAdmin || fa.Name != "" {
		t.Errorf("FullAccess() = %+v", fa)
	}
}

func TestAuthParseMode(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want Mode
		ok   bool
	}{
		{"", ModeAuto, true},
		{"auto", ModeAuto, true},
		{"required", ModeRequired, true},
		{"off", ModeOff, true},
		{"on", "", false},
		{"Off", "", false},
		{"none", "", false},
	} {
		got, err := ParseMode(tt.in)
		if tt.ok {
			if err != nil || got != tt.want {
				t.Errorf("ParseMode(%q) = %q, %v; want %q", tt.in, got, err, tt.want)
			}
			continue
		}
		if !errors.Is(err, ErrInvalidMode) || !strings.Contains(err.Error(), "auto, required, or off") {
			t.Errorf("ParseMode(%q) err = %v, want ErrInvalidMode naming the choices", tt.in, err)
		}
	}
}

func TestAuthIsLoopback(t *testing.T) {
	for host, want := range map[string]bool{
		"127.0.0.1": true, "::1": true, "[::1]": true, "localhost": true, "LOCALHOST": true,
		"0.0.0.0": false, "::": false, "": false, "127.0.0.2": false, "192.168.1.10": false,
		"example.com": false, "localhost.example.com": false,
	} {
		if got := IsLoopback(host); got != want {
			t.Errorf("IsLoopback(%q) = %v, want %v", host, got, want)
		}
	}
}

// TestAuthDecide covers every cell of the start-up matrix (approach §3).
func TestAuthDecide(t *testing.T) {
	tests := []struct {
		mode Mode
		http bool
		host string
		want Decision
	}{
		// stdio: never tokens, whatever the mode and host.
		{ModeAuto, false, "0.0.0.0", Decision{}},
		{ModeRequired, false, "0.0.0.0", Decision{}},
		{ModeOff, false, "0.0.0.0", Decision{}},
		{ModeRequired, false, "127.0.0.1", Decision{}},
		// HTTP on loopback.
		{ModeAuto, true, "127.0.0.1", Decision{}},
		{ModeAuto, true, "::1", Decision{}},
		{ModeAuto, true, "localhost", Decision{}},
		{ModeRequired, true, "127.0.0.1", Decision{TokensRequired: true}},
		{ModeOff, true, "127.0.0.1", Decision{}},
		// HTTP on any other host.
		{ModeAuto, true, "0.0.0.0", Decision{TokensRequired: true}},
		{ModeAuto, true, "192.168.1.10", Decision{TokensRequired: true}},
		{ModeRequired, true, "0.0.0.0", Decision{TokensRequired: true}},
		{ModeOff, true, "0.0.0.0", Decision{OpenNetwork: true}},
		{ModeOff, true, "db.example.com", Decision{OpenNetwork: true}},
		// An unknown mode takes the safe side.
		{Mode("bogus"), true, "127.0.0.1", Decision{TokensRequired: true}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/http=%v/%s", tt.mode, tt.http, tt.host), func(t *testing.T) {
			if got := Decide(tt.mode, tt.http, tt.host); got != tt.want {
				t.Fatalf("Decide = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestAuthLogChange(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	LogChange(logger, "root", "set_access", "alice", "database", "shop", "level", "write")
	got := buf.String()
	for _, want := range []string{"level=INFO", `msg="user management"`, "actor=root", "action=set_access", "target=alice", "database=shop", "level=write"} {
		if !strings.Contains(got, want) {
			t.Errorf("log %q lacks %q", got, want)
		}
	}
	// A nil logger falls back to slog.Default.
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	buf.Reset()
	slog.SetDefault(logger)
	LogChange(nil, "a", "delete_user", "b")
	if !strings.Contains(buf.String(), "action=delete_user") {
		t.Errorf("default logger got %q", buf.String())
	}
}

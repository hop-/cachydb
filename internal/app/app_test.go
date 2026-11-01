package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

// syncBuffer is a goroutine-safe log sink (the engine logs from
// background goroutines).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testLogger(w *syncBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestBuilderEngineOptions(t *testing.T) {
	tests := []struct {
		name      string
		configure func(b *Builder)
		wantAge   time.Duration
		wantSize  int64
		wantCache int64
	}{
		{
			name:      "unset uses engine defaults",
			configure: func(*Builder) {},
		},
		{
			name: "explicit values",
			configure: func(b *Builder) {
				b.WithWALRetention(2*time.Hour, 512<<20).WithCacheSize(32 << 20)
			},
			wantAge:   2 * time.Hour,
			wantSize:  512 << 20,
			wantCache: 32 << 20,
		},
		{
			name: "zero disables",
			configure: func(b *Builder) {
				b.WithWALRetention(0, 0).WithCacheSize(0)
			},
			wantAge:   -1,
			wantSize:  -1,
			wantCache: -1,
		},
		{
			name: "age only",
			configure: func(b *Builder) {
				b.WithWALRetention(time.Hour, 0)
			},
			wantAge:  time.Hour,
			wantSize: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBuilder().WithRootDir("/data")
			tt.configure(b)
			opts := b.EngineOptions()
			if opts.Dir != "/data" {
				t.Errorf("Dir = %q, want %q", opts.Dir, "/data")
			}
			if opts.Logger == nil {
				t.Error("Logger is nil, want a stderr logger")
			}
			if opts.WALRetentionAge != tt.wantAge {
				t.Errorf("WALRetentionAge = %v, want %v", opts.WALRetentionAge, tt.wantAge)
			}
			if opts.WALRetentionSize != tt.wantSize {
				t.Errorf("WALRetentionSize = %d, want %d", opts.WALRetentionSize, tt.wantSize)
			}
			if opts.CacheSize != tt.wantCache {
				t.Errorf("CacheSize = %d, want %d", opts.CacheSize, tt.wantCache)
			}
		})
	}
}

func TestBuildLocksDataDir(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	newBuilder := func() *Builder {
		return NewBuilder().WithRootDir(dir).WithDBName("main").WithTransport("http").WithPort(0)
	}

	a, err := newBuilder().Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if _, err := newBuilder().Build(ctx); !errors.Is(err, db.ErrDataDirInUse) {
		t.Fatalf("second Build err = %v, want ErrDataDirInUse", err)
	}

	if err := a.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := a.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	// The lock is released: the directory opens again.
	b, err := newBuilder().Build(ctx)
	if err != nil {
		t.Fatalf("Build after Stop: %v", err)
	}
	if err := b.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestBuildRejectsInvalidDBName(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	_, err := NewBuilder().WithRootDir(dir).WithDBName("../x").Build(ctx)
	if !errors.Is(err, db.ErrInvalidName) {
		t.Fatalf("Build err = %v, want ErrInvalidName", err)
	}
	// The failed Build closed the engine and released the lock.
	e, err := db.Open(ctx, db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open after failed Build: %v", err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStartStopFlushes runs the HTTP transport, cancels it like a signal
// would, stops the app, and checks that the reopened engine has the data
// without replaying anything from the WAL.
func TestStartStopFlushes(t *testing.T) {
	dir := t.TempDir()
	logs := &syncBuffer{}
	ctx := context.Background()

	a, err := NewBuilder().
		WithRootDir(dir).
		WithDBName("main").
		WithTransport("http").
		WithPort(0).
		WithLogger(testLogger(logs)).
		Build(ctx)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	d, err := a.engine.Database("main")
	if err != nil {
		t.Fatalf("Database: %v", err)
	}
	if err := d.CreateCollection(ctx, "users", nil); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	c, err := d.Collection("users")
	if err != nil {
		t.Fatalf("Collection: %v", err)
	}
	for i := range 3 {
		if _, err := c.Insert(ctx, &db.Document{Data: map[string]any{"n": i}}); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	started := make(chan error, 1)
	go func() { started <- a.Start(runCtx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}

	stopStart := time.Now()
	if err := a.Stop(context.WithoutCancel(runCtx)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(stopStart); elapsed > ShutdownTimeout {
		t.Fatalf("Stop took %v, want <= %v", elapsed, ShutdownTimeout)
	}

	reopenLogs := &syncBuffer{}
	e, err := db.Open(ctx, db.Options{Dir: dir, Logger: testLogger(reopenLogs)})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := e.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if strings.Contains(reopenLogs.String(), "recovered changes from the WAL") {
		t.Fatalf("reopen replayed the WAL after a clean stop:\n%s", reopenLogs.String())
	}
	d2, err := e.Database("main")
	if err != nil {
		t.Fatalf("Database after reopen: %v", err)
	}
	c2, err := d2.Collection("users")
	if err != nil {
		t.Fatalf("Collection after reopen: %v", err)
	}
	if got := c2.Count(); got != 3 {
		t.Fatalf("Count after reopen = %d, want 3", got)
	}
}

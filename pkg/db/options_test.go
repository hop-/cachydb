package db

import (
	"log/slog"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestOptionsWithDefaults(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	tests := []struct {
		name string
		in   Options
		want Options // Logger compared separately
	}{
		{
			name: "all defaults",
			in:   Options{Dir: "/data"},
			want: Options{
				Dir:              "/data",
				WALSegmentSize:   64 << 20,
				WALRetentionAge:  24 * time.Hour,
				WALRetentionSize: 1 << 30,
				DataSegmentSize:  64 << 20,
				FlushInterval:    time.Second,
				MemtableSize:     16 << 20,
				CacheSize:        64 << 20,
				CompactionRatio:  0.5,
				MaxPendingBytes:  defaultMaxPendingBytes(16 << 20),
				FlushWorkers:     defaultFlushWorkers(),
			},
		},
		{
			name: "explicit values kept",
			in: Options{
				Dir:              "/data",
				Logger:           logger,
				WALSegmentSize:   1,
				WALRetentionAge:  time.Minute,
				WALRetentionSize: 2,
				DataSegmentSize:  3,
				FlushInterval:    time.Millisecond,
				MemtableSize:     4,
				CacheSize:        5,
				CompactionRatio:  1,
				MaxPendingBytes:  6,
				FlushWorkers:     7,
			},
			want: Options{
				Dir:              "/data",
				WALSegmentSize:   1,
				WALRetentionAge:  time.Minute,
				WALRetentionSize: 2,
				DataSegmentSize:  3,
				FlushInterval:    time.Millisecond,
				MemtableSize:     4,
				CacheSize:        5,
				CompactionRatio:  1,
				MaxPendingBytes:  6,
				FlushWorkers:     7,
			},
		},
		{
			name: "negative disables retention and cache",
			in: Options{
				Dir:              "/data",
				WALRetentionAge:  -1,
				WALRetentionSize: -1,
				CacheSize:        -1,
				MaxPendingBytes:  -1,
			},
			want: Options{
				Dir:              "/data",
				WALSegmentSize:   64 << 20,
				WALRetentionAge:  0,
				WALRetentionSize: 0,
				DataSegmentSize:  64 << 20,
				FlushInterval:    time.Second,
				MemtableSize:     16 << 20,
				CacheSize:        0,
				CompactionRatio:  0.5,
				MaxPendingBytes:  0,
				FlushWorkers:     defaultFlushWorkers(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.in.withDefaults()
			if err != nil {
				t.Fatalf("withDefaults: %v", err)
			}
			if got.Logger == nil {
				t.Error("Logger is nil, want default")
			}
			if tt.in.Logger != nil && got.Logger != tt.in.Logger {
				t.Error("explicit Logger was replaced")
			}
			if tt.in.Logger == nil && got.Logger != slog.Default() {
				t.Error("Logger is not slog.Default()")
			}
			got.Logger = nil
			if !reflect.DeepEqual(got, tt.want) { // testHook is nil on both sides
				t.Errorf("withDefaults() =\n  %+v\nwant\n  %+v", got, tt.want)
			}
		})
	}
}

func TestDefaultMaxPendingBytes(t *testing.T) {
	procs := int64(max(1, runtime.GOMAXPROCS(0)/2))
	tests := []struct {
		memtable, want int64
	}{
		{memtable: 1 << 20, want: min(4*(1<<20)*procs, 1<<30)},
		{memtable: 1 << 40, want: 1 << 30},           // capped
		{memtable: math.MaxInt64 / 2, want: 1 << 30}, // overflow
	}
	for _, tt := range tests {
		if got := defaultMaxPendingBytes(tt.memtable); got != tt.want {
			t.Errorf("defaultMaxPendingBytes(%d) = %d, want %d", tt.memtable, got, tt.want)
		}
	}
	if w := defaultFlushWorkers(); w < 1 || w > 4 {
		t.Errorf("defaultFlushWorkers() = %d, want 1..4", w)
	}
}

func TestOptionsWithDefaultsErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      Options
		wantMsg string
	}{
		{"missing dir", Options{}, "Dir"},
		{"negative WAL segment size", Options{Dir: "d", WALSegmentSize: -1}, "WALSegmentSize"},
		{"negative data segment size", Options{Dir: "d", DataSegmentSize: -1}, "DataSegmentSize"},
		{"negative memtable size", Options{Dir: "d", MemtableSize: -1}, "MemtableSize"},
		{"negative flush interval", Options{Dir: "d", FlushInterval: -time.Second}, "FlushInterval"},
		{"negative compaction ratio", Options{Dir: "d", CompactionRatio: -0.1}, "CompactionRatio"},
		{"compaction ratio above 1", Options{Dir: "d", CompactionRatio: 1.5}, "CompactionRatio"},
		{"negative flush workers", Options{Dir: "d", FlushWorkers: -1}, "FlushWorkers"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.in.withDefaults()
			if err == nil {
				t.Fatal("withDefaults succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error = %v, want mention of %q", err, tt.wantMsg)
			}
		})
	}
}

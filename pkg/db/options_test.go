package db

import (
	"log/slog"
	"reflect"
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
			},
		},
		{
			name: "negative disables retention and cache",
			in: Options{
				Dir:              "/data",
				WALRetentionAge:  -1,
				WALRetentionSize: -1,
				CacheSize:        -1,
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

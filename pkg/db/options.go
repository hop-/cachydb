package db

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Default option values applied by Options.withDefaults to zero fields.
const (
	defaultWALSegmentSize   int64         = 64 << 20 // 64 MiB
	defaultWALRetentionAge  time.Duration = 24 * time.Hour
	defaultWALRetentionSize int64         = 1 << 30  // 1 GiB
	defaultDataSegmentSize  int64         = 64 << 20 // 64 MiB
	defaultFlushInterval    time.Duration = time.Second
	defaultMemtableSize     int64         = 16 << 20 // 16 MiB per collection
	defaultCacheSize        int64         = 64 << 20 // 64 MiB
	defaultCompactionRatio  float64       = 0.5
)

// Options configures an Engine. Zero-valued fields take the defaults
// above. For WALRetentionAge, WALRetentionSize, and CacheSize a negative
// value means "explicitly disabled" (no age limit, no size limit, cache
// off) and is normalized to 0.
type Options struct {
	Dir              string        // required: the data directory
	Logger           *slog.Logger  // nil => slog.Default() (stderr)
	WALSegmentSize   int64         // 64 MiB
	WALRetentionAge  time.Duration // 24h; negative = no age limit
	WALRetentionSize int64         // 1 GiB; negative = no size limit
	DataSegmentSize  int64         // 64 MiB
	FlushInterval    time.Duration // 1s
	MemtableSize     int64         // 16 MiB per collection
	CacheSize        int64         // 64 MiB; negative = cache disabled
	CompactionRatio  float64       // 0.5; must be in (0,1]

	testHook func(hookPoint) error // unexported; set only from export_test.go (see #crash-hooks)
	// testSegWriter, when set (tests only), wraps every active data
	// segment writer the flusher opens, to inject slow or failing I/O.
	testSegWriter func(segmentWriter) segmentWriter
}

// withDefaults fills zero fields with defaults and validates the result.
// Dir is required, sizes and intervals must be >= 0, and CompactionRatio
// must be in (0,1]. Negative WALRetentionAge, WALRetentionSize, and
// CacheSize are mapped to 0 (disabled).
func (o Options) withDefaults() (Options, error) {
	if o.Dir == "" {
		return o, errors.New("options: Dir is required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}

	o.WALSegmentSize = defaultInt64(o.WALSegmentSize, defaultWALSegmentSize)
	o.DataSegmentSize = defaultInt64(o.DataSegmentSize, defaultDataSegmentSize)
	o.MemtableSize = defaultInt64(o.MemtableSize, defaultMemtableSize)
	if o.FlushInterval == 0 {
		o.FlushInterval = defaultFlushInterval
	}

	o.WALRetentionAge = disableable(o.WALRetentionAge, defaultWALRetentionAge)
	o.WALRetentionSize = disableable(o.WALRetentionSize, defaultWALRetentionSize)
	o.CacheSize = disableable(o.CacheSize, defaultCacheSize)

	if o.CompactionRatio == 0 {
		o.CompactionRatio = defaultCompactionRatio
	}

	for _, f := range []struct {
		name string
		v    int64
	}{
		{"WALSegmentSize", o.WALSegmentSize},
		{"DataSegmentSize", o.DataSegmentSize},
		{"MemtableSize", o.MemtableSize},
		{"FlushInterval", int64(o.FlushInterval)},
	} {
		if f.v < 0 {
			return o, fmt.Errorf("options: %s must be >= 0, got %d", f.name, f.v)
		}
	}
	if !(o.CompactionRatio > 0 && o.CompactionRatio <= 1) {
		return o, fmt.Errorf("options: CompactionRatio must be in (0,1], got %v", o.CompactionRatio)
	}
	return o, nil
}

func defaultInt64(v, def int64) int64 {
	if v == 0 {
		return def
	}
	return v
}

// disableable applies def to a zero value and maps a negative value
// ("explicitly disabled") to 0.
func disableable[T int64 | time.Duration](v, def T) T {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	default:
		return v
	}
}

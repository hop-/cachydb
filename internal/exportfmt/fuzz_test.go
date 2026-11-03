package exportfmt

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hop-/cachydb/pkg/db"
)

// openQuietEngine opens an engine on dir that logs nothing. The caller
// closes it.
func openQuietEngine(t testing.TB, dir string) *db.Engine {
	t.Helper()
	e, err := db.Open(context.Background(), db.Options{Dir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

func closeEngine(t testing.TB, e *db.Engine) {
	t.Helper()
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func exportString(t testing.TB, e *db.Engine) string {
	t.Helper()
	var b bytes.Buffer
	if err := Export(context.Background(), &b, Engine{E: e}, Scope{}, "fuzz", time.Unix(0, 0)); err != nil {
		t.Fatalf("Export: %v", err)
	}
	return b.String()
}

// FuzzImportJSONL imports random JSON Lines into a fresh engine and checks
// that Import never panics, and that whatever it applied before stopping
// is a consistent state: it survives Close and reopen unchanged, and its
// export imports cleanly into another fresh engine and exports
// identically.
func FuzzImportJSONL(f *testing.F) {
	header := `{"type":"cachydb-export","version":1,"cachydb":"v0.0.5","created":"2026-11-02T00:00:00Z"}`
	valid := strings.Join([]string{
		header,
		`{"type":"database","name":"kb","schema_version":1}`,
		`{"type":"collection","database":"kb","name":"notes","schema":{"fields":{"title":{"type":"string","required":true}}},"indexes":[{"name":"by_tags","field":"tags"},{"name":"by_type","field":"type"}]}`,
		`{"type":"document","database":"kb","collection":"notes","doc":{"_id":"a","title":"A","n":1}}`,
		`{"type":"document","database":"kb","collection":"notes","doc":{"_id":"b","title":"B","tags":["x",1.5,true,null],"meta":{"z":1}}}`,
		`{"type":"collection","database":"kb","name":"empty","schema":null,"indexes":[]}`,
		`{"type":"database","name":"aa","schema_version":1}`,
	}, "\n") + "\n"
	for _, mode := range []uint8{0, 1, 2} {
		f.Add(valid, mode)
	}
	f.Add(valid+valid, uint8(1))                                                                                       // a second header
	f.Add(strings.Replace(valid, `"_id":"b"`, `"_id":"a"`, 1), uint8(0))                                               // duplicate id
	f.Add(strings.Replace(valid, `"title":"B",`, ``, 1), uint8(2))                                                     // schema violation
	f.Add(header+"\n"+`{"type":"document","database":"nope","collection":"x","doc":{"_id":"a"}}`, uint8(0))            // missing database
	f.Add(header+"\n"+`{"type":"collection","database":"kb","name":"c","indexes":[{"name":"","field":""}]}`, uint8(0)) // bad index
	f.Add(header+"\n"+`{"type":"database","name":"../x","schema_version":1}`, uint8(0))                                // bad name
	f.Add(valid[:len(valid)/2], uint8(0))                                                                              // truncated line
	f.Add("", uint8(0))
	f.Add("\n\n", uint8(0))

	modes := []OnConflict{Fail, Skip, Replace}
	f.Fuzz(func(t *testing.T, input string, mode uint8) {
		ctx := context.Background()
		dir := t.TempDir()
		e := openQuietEngine(t, dir)
		st, err := Import(ctx, strings.NewReader(input), Engine{E: e}, modes[int(mode)%len(modes)])
		if st.Inserted+st.Replaced+st.Skipped+st.Databases+st.Collections > st.Lines {
			t.Fatalf("stats %+v count more than the lines applied", st)
		}
		if err == nil && st.Lines == 0 {
			t.Fatalf("Import of nothing succeeded")
		}
		before := exportString(t, e)
		closeEngine(t, e)

		e = openQuietEngine(t, dir)
		after := exportString(t, e)
		closeEngine(t, e)
		if after != before {
			t.Fatalf("state changed across reopen:\n%s\nvs\n%s", before, after)
		}

		other := openQuietEngine(t, t.TempDir())
		defer closeEngine(t, other)
		if _, err := Import(ctx, strings.NewReader(before), Engine{E: other}, Fail); err != nil {
			t.Fatalf("re-importing the applied state: %v\n%s", err, before)
		}
		if again := exportString(t, other); again != before {
			t.Fatalf("applied state does not round-trip:\n%s\nvs\n%s", before, again)
		}
	})
}

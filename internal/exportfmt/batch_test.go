package exportfmt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hop-/cachydb/pkg/db"
)

// Tests of the batch import (plan 013 §3.1, "Users of Batch"): document
// lines of one collection go to a BatchTarget in runs.

// countingTarget is the engine adapter that records how documents reach
// it.
type countingTarget struct {
	Engine
	mu      sync.Mutex
	batches []int // documents per InsertDocuments call
	singles int   // InsertDocument calls
}

func (c *countingTarget) InsertDocuments(ctx context.Context, database, collection string, docs []map[string]any, mode OnConflict) (Stats, error) {
	c.mu.Lock()
	c.batches = append(c.batches, len(docs))
	c.mu.Unlock()
	return c.Engine.InsertDocuments(ctx, database, collection, docs, mode)
}

func (c *countingTarget) InsertDocument(ctx context.Context, database, collection string, doc map[string]any) error {
	c.mu.Lock()
	c.singles++
	c.mu.Unlock()
	return c.Engine.InsertDocument(ctx, database, collection, doc)
}

// plainTarget hides InsertDocuments: a Target without batches.
type plainTarget struct{ Target }

const exportHeader = `{"type":"cachydb-export","version":1}`

// exportOf returns an export of database d, collection c, holding the
// given document lines.
func exportOf(docs ...string) string {
	var b strings.Builder
	b.WriteString(exportHeader + "\n" + `{"type":"database","name":"d"}` + "\n" + `{"type":"collection","database":"d","name":"c"}` + "\n")
	for _, d := range docs {
		fmt.Fprintf(&b, `{"type":"document","database":"d","collection":"c","doc":%s}`+"\n", d)
	}
	return b.String()
}

// manyDocs returns n document objects x00000.. with the field n.
func manyDocs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"_id":"x%05d","n":%d}`, i, i)
	}
	return out
}

func openEngineWith(t *testing.T, mod func(*db.Options)) *db.Engine {
	t.Helper()
	o := db.Options{Dir: t.TempDir()}
	if mod != nil {
		mod(&o)
	}
	e, err := db.Open(context.Background(), o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close(context.Background()) })
	return e
}

func countDocs(t *testing.T, e *db.Engine, database, collection string) int {
	t.Helper()
	c, err := mustColl(t, e, database, collection)
	if err != nil {
		t.Fatal(err)
	}
	return c.Count()
}

func TestImportInBatches(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, t.TempDir())
	tgt := &countingTarget{Engine: Engine{E: e}}
	st, err := Import(ctx, strings.NewReader(exportOf(manyDocs(2500)...)), tgt, Fail)
	if err != nil || st.Inserted != 2500 || st.Lines != 2503 {
		t.Fatalf("Import = %+v, %v", st, err)
	}
	if fmt.Sprint(tgt.batches) != "[1000 1000 500]" || tgt.singles != 0 {
		t.Errorf("batches %v, single inserts %d; want [1000 1000 500] and none", tgt.batches, tgt.singles)
	}
	if n := countDocs(t, e, "d", "c"); n != 2500 {
		t.Errorf("Count = %d", n)
	}

	// The same export through a Target without batches gives the same
	// result.
	e2 := openEngine(t, t.TempDir())
	st2, err := Import(ctx, strings.NewReader(exportOf(manyDocs(30)...)), plainTarget{Engine{E: e2}}, Fail)
	if err != nil || st2.Inserted != 30 || st2.Lines != 33 {
		t.Errorf("Import without batches = %+v, %v", st2, err)
	}
}

// TestImportRunBoundaries: a run is written before a line of another
// collection or kind, before a repeated _id, and at the end.
func TestImportRunBoundaries(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, t.TempDir())
	tgt := &countingTarget{Engine: Engine{E: e}}
	in := exportHeader + "\n" +
		`{"type":"database","name":"d"}` + "\n" +
		`{"type":"collection","database":"d","name":"c"}` + "\n" +
		`{"type":"collection","database":"d","name":"o"}` + "\n" +
		`{"type":"document","database":"d","collection":"c","doc":{"_id":"a"}}` + "\n" +
		`{"type":"document","database":"d","collection":"c","doc":{"_id":"b"}}` + "\n" +
		`{"type":"document","database":"d","collection":"o","doc":{"_id":"a"}}` + "\n" +
		`{"type":"document","database":"d","collection":"c","doc":{"_id":"c"}}` + "\n" +
		`{"type":"database","name":"e"}` + "\n" +
		`{"type":"collection","database":"e","name":"c"}` + "\n" +
		`{"type":"document","database":"e","collection":"c","doc":{"_id":"z"}}` + "\n" +
		"\n" + // blank lines are skipped
		`{"type":"document","database":"e","collection":"c","doc":{"_id":"y"}}` + "\n"
	st, err := Import(ctx, strings.NewReader(in), tgt, Fail)
	if err != nil || st.Inserted != 6 || st.Lines != 12 || st.Databases != 2 || st.Collections != 3 {
		t.Fatalf("Import = %+v, %v", st, err)
	}
	if fmt.Sprint(tgt.batches) != "[2 1 1 2]" {
		t.Errorf("batches %v, want [2 1 1 2]", tgt.batches)
	}
	for _, w := range []struct {
		d, c string
		n    int
	}{{"d", "c", 3}, {"d", "o", 1}, {"e", "c", 2}} {
		if got := countDocs(t, e, w.d, w.c); got != w.n {
			t.Errorf("%s/%s holds %d documents, want %d", w.d, w.c, got, w.n)
		}
	}
}

func TestImportBatchConflicts(t *testing.T) {
	ctx := context.Background()
	data := exportOf(manyDocs(2500)...)

	t.Run("fail names the line", func(t *testing.T) {
		e := openEngine(t, t.TempDir())
		bad := data + `{"type":"document","database":"d","collection":"c","doc":{"_id":"new1"}}` + "\n" +
			`{"type":"document","database":"d","collection":"c","doc":{"_id":"x00007"}}` + "\n"
		st, err := Import(ctx, strings.NewReader(bad), Engine{E: e}, Fail)
		// The first 2000 documents are in two full batches; the third
		// batch (x02000..x02499, new1, x00007 repeats an id of an earlier
		// batch) fails whole at x00007's line.
		if err == nil || !strings.Contains(err.Error(), "line 2505 (2003 lines applied)") ||
			!strings.Contains(err.Error(), `document "x00007" in d/c already exists`) {
			t.Fatalf("Import = %+v, %v", st, err)
		}
		if st.Lines != 2003 || st.Inserted != 2000 {
			t.Errorf("stats %+v, want 2003 lines and 2000 documents applied", st)
		}
		if n := countDocs(t, e, "d", "c"); n != 2000 {
			t.Errorf("Count = %d, want 2000: the failing batch must not be applied", n)
		}
	})
	t.Run("skip and replace with a repeated id", func(t *testing.T) {
		e := openEngine(t, t.TempDir())
		if _, err := Import(ctx, strings.NewReader(exportOf(`{"_id":"x1","v":0}`)), Engine{E: e}, Fail); err != nil {
			t.Fatal(err)
		}
		dup := exportOf(`{"_id":"x1","v":1}`, `{"_id":"y","v":1}`, `{"_id":"y","v":2}`)
		st, err := Import(ctx, strings.NewReader(dup), Engine{E: e}, Replace)
		if err != nil || st.Replaced != 2 || st.Inserted != 1 {
			t.Fatalf("Replace = %+v, %v", st, err)
		}
		c, _ := mustColl(t, e, "d", "c")
		if d, _ := c.Get(ctx, "y"); d.Data["v"] != 2.0 {
			t.Errorf("y = %v, want the last line's version", d.Data)
		}
		st, err = Import(ctx, strings.NewReader(dup), Engine{E: e}, Skip)
		if err != nil || st.Skipped != 3 || st.Inserted != 0 {
			t.Fatalf("Skip = %+v, %v", st, err)
		}
		if d, _ := c.Get(ctx, "x1"); d.Data["v"] != 1.0 {
			t.Errorf("x1 = %v after skip", d.Data)
		}
	})
	t.Run("a failing document names its line", func(t *testing.T) {
		e := openEngine(t, t.TempDir())
		d, err := e.CreateDatabase(ctx, "d")
		if err != nil {
			t.Fatal(err)
		}
		if err := d.CreateCollection(ctx, "c", &db.Schema{Fields: map[string]db.Field{"n": {Type: db.TypeNumber}}}); err != nil {
			t.Fatal(err)
		}
		in := exportHeader + "\n" +
			`{"type":"document","database":"d","collection":"c","doc":{"_id":"a","n":1}}` + "\n" +
			`{"type":"document","database":"d","collection":"c","doc":{"_id":"b","n":"text"}}` + "\n" +
			`{"type":"document","database":"d","collection":"c","doc":{"_id":"c","n":3}}` + "\n"
		for _, mode := range []OnConflict{Fail, Skip, Replace} {
			st, err := Import(ctx, strings.NewReader(in), Engine{E: e}, mode)
			if !errors.Is(err, db.ErrSchemaValidation) || !strings.Contains(err.Error(), `import line 3 (1 lines applied): document "b" in d/c`) {
				t.Errorf("%s: Import = %+v, %v", mode, st, err)
			}
		}
		if n := countDocs(t, e, "d", "c"); n != 0 {
			t.Errorf("Count = %d: a failing batch was applied", n)
		}
	})
	t.Run("a malformed line after pending documents", func(t *testing.T) {
		e := openEngine(t, t.TempDir())
		for name, line := range map[string]string{
			"not a document": `{"type":"document","doc":{}}`,
			"invalid JSON":   `{"type":"document","database":"q","collection":"c","doc":5}`,
		} {
			in := exportHeader + "\n" + `{"type":"database","name":"q"}` + "\n" + `{"type":"collection","database":"q","name":"c"}` + "\n" +
				`{"type":"document","database":"q","collection":"c","doc":{"_id":"` + name + `"}}` + "\n" + line + "\n"
			st, err := Import(ctx, strings.NewReader(in), Engine{E: e}, Replace)
			if err == nil || !strings.Contains(err.Error(), "line 5 (4 lines applied)") {
				t.Errorf("%s: Import = %+v, %v", name, st, err)
			}
		}
		if n := countDocs(t, e, "q", "c"); n != 2 {
			t.Errorf("Count = %d, want the 2 pending documents applied before the errors", n)
		}
	})
	t.Run("a pending run fails before a later line", func(t *testing.T) {
		e := openEngine(t, t.TempDir())
		if _, err := Import(ctx, strings.NewReader(exportOf(`{"_id":"a"}`)), Engine{E: e}, Fail); err != nil {
			t.Fatal(err)
		}
		in := exportOf(`{"_id":"b"}`, `{"_id":"a"}`) + `{"type":"database","name":"later"}` + "\n"
		st, err := Import(ctx, strings.NewReader(in), Engine{E: e}, Fail)
		if err == nil || !strings.Contains(err.Error(), `import line 5 (3 lines applied): document "a" in d/c already exists`) {
			t.Errorf("Import = %+v, %v", st, err)
		}
		if _, err := e.Database("later"); !errors.Is(err, db.ErrNotFound) {
			t.Errorf("the line after the failing run was applied: %v", err)
		}
	})
}

// TestImportSplitsLargeRuns: a run over the target's batch limits is
// split in halves, and a single document over them goes through
// InsertDocument.
func TestImportSplitsLargeRuns(t *testing.T) {
	ctx := context.Background()
	t.Run("MaxBatchDocs", func(t *testing.T) {
		e := openEngineWith(t, func(o *db.Options) { o.MaxBatchDocs = 3 })
		tgt := &countingTarget{Engine: Engine{E: e}}
		st, err := Import(ctx, strings.NewReader(exportOf(manyDocs(10)...)), tgt, Fail)
		if err != nil || st.Inserted != 10 || st.Lines != 13 {
			t.Fatalf("Import = %+v, %v", st, err)
		}
		// 10 → 5 + 5 → 2 + 3 + 2 + 3.
		if fmt.Sprint(tgt.batches) != "[10 5 2 3 5 2 3]" || tgt.singles != 0 {
			t.Errorf("batches %v, singles %d", tgt.batches, tgt.singles)
		}
		if n := countDocs(t, e, "d", "c"); n != 10 {
			t.Errorf("Count = %d", n)
		}
	})
	t.Run("MaxBatchBytes", func(t *testing.T) {
		e := openEngineWith(t, func(o *db.Options) { o.MaxBatchBytes = 300 })
		tgt := &countingTarget{Engine: Engine{E: e}}
		big := `{"_id":"big","pad":"` + strings.Repeat("x", 500) + `"}`
		docs := append(manyDocs(4), big)
		for _, mode := range []OnConflict{Fail, Replace} {
			tgt.batches, tgt.singles = nil, 0
			st, err := Import(ctx, strings.NewReader(exportOf(docs...)), tgt, mode)
			if err != nil || st.Lines != 8 || st.Inserted+st.Replaced != 5 {
				t.Fatalf("%s: Import = %+v, %v", mode, st, err)
			}
			if tgt.singles != 1 {
				t.Errorf("%s: %d single inserts, want 1 (the document over MaxBatchBytes); batches %v", mode, tgt.singles, tgt.batches)
			}
		}
		c, _ := mustColl(t, e, "d", "c")
		if d, err := c.Get(ctx, "big"); err != nil || len(d.Data["pad"].(string)) != 500 {
			t.Errorf("big = %v, %v", d, err)
		}
		// A single document over the limit that exists, in Fail mode.
		st, err := Import(ctx, strings.NewReader(exportOf(big)), tgt, Fail)
		if err == nil || !strings.Contains(err.Error(), `line 4 (3 lines applied): document "big" in d/c already exists`) {
			t.Errorf("Import of an existing large document = %+v, %v", st, err)
		}
	})
}

// TestEngineInsertDocuments: the engine adapter's batch, and the index of
// a failing document in docs.
func TestEngineInsertDocuments(t *testing.T) {
	ctx := context.Background()
	e := openEngine(t, t.TempDir())
	a := Engine{E: e}
	if _, err := a.InsertDocuments(ctx, "d", "c", []map[string]any{{"_id": "x"}}, Fail); !errors.Is(err, db.ErrNotFound) {
		t.Errorf("InsertDocuments into a missing database: %v", err)
	}
	if err := a.EnsureDatabase(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	if err := a.CreateCollection(ctx, "d", "c", &db.Schema{Fields: map[string]db.Field{"n": {Type: db.TypeNumber}}}); err != nil {
		t.Fatal(err)
	}
	st, err := a.InsertDocuments(ctx, "d", "c", []map[string]any{{"_id": "a", "n": 1.0}, {"_id": "b", "n": 2.0}}, Fail)
	if err != nil || st.Inserted != 2 {
		t.Fatalf("InsertDocuments = %+v, %v", st, err)
	}
	// Fail: an existing id wraps ErrDocumentExists and names its position.
	_, err = a.InsertDocuments(ctx, "d", "c", []map[string]any{{"_id": "c"}, {"_id": "b"}}, Fail)
	bie, ok := errors.AsType[*db.BatchItemError](err)
	if !ok || bie.Index != 1 || !errors.Is(err, ErrDocumentExists) || !errors.Is(err, db.ErrExists) {
		t.Errorf("Fail with an existing id = %v", err)
	}
	// Skip: the position in docs, not in the batch, of a failing document.
	_, err = a.InsertDocuments(ctx, "d", "c", []map[string]any{{"_id": "a"}, {"_id": "new"}, {"_id": "bad", "n": "x"}}, Skip)
	bie, ok = errors.AsType[*db.BatchItemError](err)
	if !ok || bie.Index != 2 || !errors.Is(err, db.ErrSchemaValidation) {
		t.Errorf("Skip with an invalid document = %v", err)
	}
	st, err = a.InsertDocuments(ctx, "d", "c", []map[string]any{{"_id": "a", "n": 9.0}, {"_id": "new"}}, Replace)
	if err != nil || st.Replaced != 1 || st.Inserted != 1 {
		t.Errorf("Replace = %+v, %v", st, err)
	}
	c, _ := mustColl(t, e, "d", "c")
	if d, _ := c.Get(ctx, "a"); d.Data["n"] != 9.0 || c.Count() != 3 {
		t.Errorf("after Replace: a = %v, Count %d", d.Data, c.Count())
	}
	st, err = a.InsertDocuments(ctx, "d", "c", []map[string]any{{"_id": "a"}, {"_id": "new"}}, Skip)
	if err != nil || st.Skipped != 2 || st.Inserted != 0 {
		t.Errorf("Skip of existing documents = %+v, %v", st, err)
	}
}

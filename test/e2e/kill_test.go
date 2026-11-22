//go:build e2e

// Package e2e holds end-to-end tests that run the real cachydb binary.
// They are behind the e2e build tag:
//
//	TMPDIR=$HOME/.cache/cachydb-e2e go test -tags e2e -run TestKill ./test/e2e/
//
// Run them on a real disk (not tmpfs), so fsync is real. Knobs:
// CACHYDB_E2E_ROUNDS (default 10) and CACHYDB_E2E_ARTIFACTS (a directory
// that receives the data directory and the server logs of a failed run).
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	mrand "math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	e2eDB         = "e2e"
	docsColl      = "docs"
	countersColl  = "counters"
	writers       = 4
	maxLivePerW   = 150 // a writer deletes instead of inserting above this
	cleanStopEach = 5   // every 5th round stops with SIGTERM instead of SIGKILL
)

// ---- harness ----

// buildBinary builds cachydb once for the test.
func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "cachydb")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/hop-/cachydb")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building cachydb: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// server is one run of the cachydb binary.
type server struct {
	cmd     *exec.Cmd
	log     string // path of the combined stdout/stderr log
	url     string // MCP endpoint
	health  string
	exited  chan struct{}
	waitErr error
}

func startServer(t *testing.T, bin, dir, logDir string, run, port int, extra ...string) *server {
	t.Helper()
	logPath := filepath.Join(logDir, fmt.Sprintf("server-%03d.log", run))
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"-t", "http", "-p", strconv.Itoa(port), "-R", dir, "-d", e2eDB}, extra...)
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = append(os.Environ(), "CACHYDB_URL=") // no stray client settings
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting cachydb: %v", err)
	}
	s := &server{
		cmd:    cmd,
		log:    logPath,
		url:    fmt.Sprintf("http://127.0.0.1:%d/mcp", port),
		health: fmt.Sprintf("http://127.0.0.1:%d/healthz", port),
		exited: make(chan struct{}),
	}
	go func() {
		s.waitErr = cmd.Wait()
		_ = logFile.Close() // the log is read after exit; a close error loses nothing written
		close(s.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-s.exited:
		default:
			_ = cmd.Process.Kill() // test is ending; the process must not outlive it
			<-s.exited
		}
	})

	// Wait for the health probe of the binary itself to succeed.
	deadline := time.Now().Add(20 * time.Second)
	for {
		probe := exec.Command(bin, "healthcheck", "--url", s.health)
		if probe.Run() == nil {
			return s
		}
		select {
		case <-s.exited:
			t.Fatalf("cachydb exited during startup (%v); log:\n%s", s.waitErr, readFile(t, logPath))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("cachydb not healthy within 20s; log:\n%s", readFile(t, logPath))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// stop sends sig and waits for the process to exit.
func (s *server) stop(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := s.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signal %v: %v", sig, err)
	}
	select {
	case <-s.exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("cachydb did not exit within 30s of %v", sig)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// checkLog fails on any ERROR-level line in a server log. Torn-tail
// truncations after a kill are WARN and allowed.
func checkLog(t *testing.T, path string) {
	t.Helper()
	sc := bufio.NewScanner(strings.NewReader(readFile(t, path)))
	for sc.Scan() {
		if strings.Contains(sc.Text(), "level=ERROR") {
			t.Errorf("%s: %s", filepath.Base(path), sc.Text())
		}
	}
}

// keepArtifacts copies the data directory and the logs to
// $CACHYDB_E2E_ARTIFACTS when the test fails.
func keepArtifacts(t *testing.T, dirs ...string) {
	t.Cleanup(func() {
		root := os.Getenv("CACHYDB_E2E_ARTIFACTS")
		if !t.Failed() || root == "" {
			return
		}
		dst := filepath.Join(root, fmt.Sprintf("%s-%d", t.Name(), time.Now().Unix()))
		for _, dir := range dirs {
			if err := copyTree(dir, filepath.Join(dst, filepath.Base(dir))); err != nil {
				t.Logf("keeping artifacts: %v", err)
			}
		}
		t.Logf("artifacts kept in %s", dst)
	})
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// ---- MCP client ----

type client struct{ cs *mcp.ClientSession }

func connect(ctx context.Context, url string) (*client, error) {
	c := mcp.NewClient(&mcp.Implementation{Name: "cachydb-e2e", Version: "test"}, nil)
	// No retries: a request is sent at most once, so an error means "may
	// or may not have landed", never "landed twice".
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if err != nil {
		return nil, err
	}
	return &client{cs: cs}, nil
}

var errTool = errors.New("tool error")

// call calls tool and decodes its structured result into out (if not nil).
func (c *client) call(ctx context.Context, tool string, args map[string]any, out any) error {
	res, err := c.cs.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return fmt.Errorf("calling %s: %w", tool, err)
	}
	if res.IsError {
		var msg []string
		for _, ct := range res.Content {
			if tc, ok := ct.(*mcp.TextContent); ok {
				msg = append(msg, tc.Text)
			}
		}
		return fmt.Errorf("%w: %s: %s", errTool, tool, strings.Join(msg, "; "))
	}
	if out == nil {
		return nil
	}
	data, err := json.Marshal(res.StructuredContent)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func (c *client) close() { _ = c.cs.Close() } // the server may be gone already

// ---- acknowledged state ----

// docState is what the test knows about one document: its acknowledged
// version (nil = absent) and, while a write is in flight or ended without
// an answer, the version that write would leave (pending).
type docState struct {
	acked      map[string]any
	hasPending bool
	pending    map[string]any
	// batch names the batch call (insert_documents, update_documents,
	// delete_documents) of the pending write, "" for a single write. The
	// pending writes of one batch must all land or none (verify).
	batch string
}

// writerState is owned by one writer goroutine during a round and read
// by the test between rounds.
type writerState struct {
	id      int
	seq     int
	docs    map[string]*docState
	live    []string // ids acknowledged as present, for updates and deletes
	counter float64  // acknowledged counter value
	// counterPending is set while a counter update is unanswered.
	counterPending bool
	ops            map[string]int
	batches        int // batch calls started, for docState.batch names
}

func newWriterState(id int) *writerState {
	return &writerState{id: id, docs: map[string]*docState{}, ops: map[string]int{}}
}

func normalize(v map[string]any) map[string]any {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b) // crypto/rand.Read never fails
	return hex.EncodeToString(b)
}

// run writes until an error (the kill) or ctx ends. It returns an error
// only for a failure that happened before the server was stopped.
func (w *writerState) run(ctx context.Context, url string, round int, rng *mrand.Rand, stopped *atomic.Bool) error {
	c, err := connect(ctx, url)
	if err != nil {
		if stopped.Load() || ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("writer %d: connecting: %w", w.id, err)
	}
	defer c.close()
	counterID := fmt.Sprintf("w%d", w.id)
	for ctx.Err() == nil {
		var err error
		switch p := rng.IntN(100); {
		case p < 40 && len(w.live) < maxLivePerW:
			w.seq++
			id := fmt.Sprintf("r%03d-w%d-%05d", round, w.id, w.seq)
			doc := normalize(map[string]any{"_id": id, "w": w.id, "round": round, "seq": w.seq, "payload": randHex(64 + rng.IntN(96))})
			st := &docState{hasPending: true, pending: doc}
			w.docs[id] = st
			err = c.call(ctx, "insert_document", map[string]any{"collection": docsColl, "document": doc}, nil)
			if err == nil {
				st.acked, st.hasPending, st.pending = doc, false, nil
				w.live = append(w.live, id)
				w.ops["insert"]++
			}
		case p < 50 && len(w.live) < maxLivePerW:
			err = w.insertBatch(ctx, c, round, rng)
		case p < 62 && len(w.live) > 0:
			id := w.live[rng.IntN(len(w.live))]
			st := w.docs[id]
			next := normalize(st.acked)
			next["v"] = rng.IntN(1000)
			next["payload"] = randHex(32)
			next = normalize(next)
			st.hasPending, st.pending = true, next
			err = c.call(ctx, "update_document", map[string]any{"collection": docsColl, "id": id,
				"updates": map[string]any{"v": next["v"], "payload": next["payload"]}}, nil)
			if err == nil {
				st.acked, st.hasPending, st.pending = next, false, nil
				w.ops["update"]++
			}
		case p < 70 && len(w.live) > 1:
			err = w.updateBatch(ctx, c, rng)
		case p < 80 && len(w.live) > 0:
			i := rng.IntN(len(w.live))
			id := w.live[i]
			st := w.docs[id]
			st.hasPending, st.pending = true, nil
			err = c.call(ctx, "delete_document", map[string]any{"collection": docsColl, "id": id}, nil)
			if err == nil {
				st.acked, st.hasPending = nil, false
				w.live = append(w.live[:i], w.live[i+1:]...)
				w.ops["delete"]++
			}
		case p < 85 && len(w.live) > 1:
			err = w.deleteBatch(ctx, c, rng)
		default:
			// Conditional increment of this writer's counter.
			w.counterPending = true
			err = c.call(ctx, "update_document", map[string]any{"collection": countersColl, "id": counterID,
				"updates": map[string]any{"n": w.counter + 1},
				"if":      []map[string]any{{"field": "n", "operator": "eq", "value": w.counter}}}, nil)
			if err == nil {
				w.counter++
				w.counterPending = false
				w.ops["counter"]++
			}
		}
		if err != nil {
			if stopped.Load() || ctx.Err() != nil {
				return nil // the kill (or shutdown): the write is in flight
			}
			return fmt.Errorf("writer %d: %w", w.id, err)
		}
	}
	return nil
}

// pickLive returns n distinct ids from w.live (2 <= n <= len(w.live)),
// with their positions in w.live.
func (w *writerState) pickLive(rng *mrand.Rand, n int) ([]string, []int) {
	perm := rng.Perm(len(w.live))[:n]
	ids := make([]string, n)
	for i, j := range perm {
		ids[i] = w.live[j]
	}
	return ids, perm
}

// removeLive removes the ids from w.live.
func (w *writerState) removeLive(ids []string) {
	gone := make(map[string]bool, len(ids))
	for _, id := range ids {
		gone[id] = true
	}
	kept := w.live[:0]
	for _, id := range w.live {
		if !gone[id] {
			kept = append(kept, id)
		}
	}
	w.live = kept
}

// nextBatch returns a new batch name for docState.batch.
func (w *writerState) nextBatch(kind string) string {
	w.batches++
	return fmt.Sprintf("w%d-%s-%d", w.id, kind, w.batches)
}

// insertBatch inserts 2 to 5 new documents with one insert_documents call
// (one atomic batch WAL record).
func (w *writerState) insertBatch(ctx context.Context, c *client, round int, rng *mrand.Rand) error {
	n := 2 + rng.IntN(4)
	batch := w.nextBatch("insert")
	docs := make([]map[string]any, n)
	ids := make([]string, n)
	states := make([]*docState, n)
	for i := range docs {
		w.seq++
		ids[i] = fmt.Sprintf("r%03d-w%d-%05d", round, w.id, w.seq)
		docs[i] = normalize(map[string]any{"_id": ids[i], "w": w.id, "round": round, "seq": w.seq, "batch": batch, "payload": randHex(32 + rng.IntN(64))})
		states[i] = &docState{hasPending: true, pending: docs[i], batch: batch}
		w.docs[ids[i]] = states[i]
	}
	var out struct {
		IDs      []string `json:"ids"`
		Inserted int      `json:"inserted"`
	}
	if err := c.call(ctx, "insert_documents", map[string]any{"collection": docsColl, "documents": docs}, &out); err != nil {
		return err
	}
	if out.Inserted != n || !reflect.DeepEqual(out.IDs, ids) {
		return fmt.Errorf("insert_documents %s: inserted %d, ids %v; want %d, %v", batch, out.Inserted, out.IDs, n, ids)
	}
	for i, st := range states {
		st.acked, st.hasPending, st.pending, st.batch = docs[i], false, nil, ""
	}
	w.live = append(w.live, ids...)
	w.ops["insert_documents"]++
	return nil
}

// updateBatch updates 2 to 4 live documents with one update_documents call
// by _id: a field set and an inc operator.
func (w *writerState) updateBatch(ctx context.Context, c *client, rng *mrand.Rand) error {
	ids, _ := w.pickLive(rng, min(2+rng.IntN(3), len(w.live)))
	batch := w.nextBatch("update")
	v := rng.IntN(1000)
	payload := randHex(24)
	nexts := make([]map[string]any, len(ids))
	for i, id := range ids {
		st := w.docs[id]
		next := normalize(st.acked)
		next["v"] = v
		next["payload"] = payload
		hits, _ := next["hits"].(float64) // a missing field starts at 0
		next["hits"] = hits + 1
		nexts[i] = normalize(next)
		st.hasPending, st.pending, st.batch = true, nexts[i], batch
	}
	var out struct {
		Matched  int `json:"matched"`
		Modified int `json:"modified"`
	}
	err := c.call(ctx, "update_documents", map[string]any{"collection": docsColl,
		"query":   map[string]any{"filters": []map[string]any{{"field": "_id", "operator": "in", "value": ids}}},
		"updates": map[string]any{"v": v, "payload": payload},
		"inc":     map[string]any{"hits": 1}}, &out)
	if err != nil {
		return err
	}
	if out.Matched != len(ids) || out.Modified != len(ids) {
		return fmt.Errorf("update_documents %s: matched %d, modified %d; want %d", batch, out.Matched, out.Modified, len(ids))
	}
	for i, id := range ids {
		st := w.docs[id]
		st.acked, st.hasPending, st.pending, st.batch = nexts[i], false, nil, ""
	}
	w.ops["update_documents"]++
	return nil
}

// deleteBatch deletes 2 to 3 live documents with one delete_documents call
// by _id.
func (w *writerState) deleteBatch(ctx context.Context, c *client, rng *mrand.Rand) error {
	ids, _ := w.pickLive(rng, min(2+rng.IntN(2), len(w.live)))
	batch := w.nextBatch("delete")
	for _, id := range ids {
		st := w.docs[id]
		st.hasPending, st.pending, st.batch = true, nil, batch
	}
	var out struct {
		Matched int `json:"matched"`
		Deleted int `json:"deleted"`
	}
	err := c.call(ctx, "delete_documents", map[string]any{"collection": docsColl,
		"query": map[string]any{"filters": []map[string]any{{"field": "_id", "operator": "in", "value": ids}}}}, &out)
	if err != nil {
		return err
	}
	if out.Matched != len(ids) || out.Deleted != len(ids) {
		return fmt.Errorf("delete_documents %s: matched %d, deleted %d; want %d", batch, out.Matched, out.Deleted, len(ids))
	}
	for _, id := range ids {
		st := w.docs[id]
		st.acked, st.hasPending, st.batch = nil, false, ""
	}
	w.removeLive(ids)
	w.ops["delete_documents"]++
	return nil
}

// ---- verification ----

type findOutput struct {
	Documents []map[string]any `json:"documents"`
	HasMore   bool             `json:"has_more"`
	NextSkip  int              `json:"next_skip"`
}

// findAll reads every document find_documents args matches, a page at a
// time: it follows has_more and next_skip (limit 0 means the server's
// default limit). It fails the test if next_skip does not continue right
// after the page, or a document comes back twice.
func findAll(t *testing.T, ctx context.Context, c *client, args map[string]any, limit int) []map[string]any {
	t.Helper()
	var all []map[string]any
	seen := map[string]bool{}
	skip := 0
	for page := 0; ; page++ {
		query := map[string]any{"skip": skip}
		if limit > 0 {
			query["limit"] = limit
		}
		call := map[string]any{"query": query}
		for k, v := range args {
			call[k] = v
		}
		var out findOutput
		if err := c.call(ctx, "find_documents", call, &out); err != nil {
			t.Fatalf("find_documents %v (page %d): %v", args, page, err)
		}
		if limit > 0 && len(out.Documents) > limit {
			t.Fatalf("find_documents %v: page %d has %d documents, limit %d", args, page, len(out.Documents), limit)
		}
		for _, d := range out.Documents {
			id, _ := d["_id"].(string)
			if seen[id] {
				t.Fatalf("find_documents %v: document %s returned twice while paging", args, id)
			}
			seen[id] = true
		}
		all = append(all, out.Documents...)
		if !out.HasMore {
			return all
		}
		if len(out.Documents) == 0 || out.NextSkip != skip+len(out.Documents) {
			t.Fatalf("find_documents %v: page %d at skip %d returned %d documents, has_more with next_skip %d", args, page, skip, len(out.Documents), out.NextSkip)
		}
		skip = out.NextSkip
	}
}

type countOutput struct {
	Count int `json:"count"`
}

// verify checks the restarted server against every writer's
// acknowledged state, then resolves in-flight writes to what landed.
func verify(t *testing.T, ctx context.Context, url string, ws []*writerState) {
	t.Helper()
	c, err := connect(ctx, url)
	if err != nil {
		t.Fatalf("verify: connecting: %v", err)
	}
	defer c.close()

	// Every database and collection opens and counts like a scan.
	var dbs struct {
		Databases []string `json:"databases"`
	}
	if err := c.call(ctx, "list_databases", map[string]any{}, &dbs); err != nil {
		t.Fatalf("list_databases: %v", err)
	}
	for _, d := range dbs.Databases {
		var colls struct {
			Collections []string `json:"collections"`
		}
		if err := c.call(ctx, "list_collections", map[string]any{"database": d}, &colls); err != nil {
			t.Fatalf("list_collections %s: %v", d, err)
		}
		for _, coll := range colls.Collections {
			if err := c.call(ctx, "describe_collection", map[string]any{"database": d, "collection": coll}, nil); err != nil {
				t.Errorf("describe_collection %s/%s: %v", d, coll, err)
			}
			// Pages of the server's default size (limit omitted).
			all := findAll(t, ctx, c, map[string]any{"database": d, "collection": coll}, 0)
			var n countOutput
			if err := c.call(ctx, "count_documents", map[string]any{"database": d, "collection": coll}, &n); err != nil {
				t.Fatalf("count_documents %s/%s: %v", d, coll, err)
			}
			if n.Count != len(all) {
				t.Errorf("%s/%s: count_documents %d, a scan finds %d", d, coll, n.Count, len(all))
			}
		}
	}

	// Pages of an explicit, odd size.
	all := findAll(t, ctx, c, map[string]any{"collection": docsColl}, 37)
	stored := make(map[string]map[string]any, len(all))
	perWriter := map[float64]int{}
	// landed records, per batch, whether each of its pending writes
	// landed: all or none may.
	landed := map[string][]bool{}
	for _, d := range all {
		id, _ := d["_id"].(string)
		stored[id] = d
		if w, ok := d["w"].(float64); ok {
			perWriter[w]++
		}
	}
	for _, w := range ws {
		// The index on "w" answers like the scan.
		var n countOutput
		if err := c.call(ctx, "count_documents", map[string]any{"collection": docsColl,
			"query": map[string]any{"filters": []map[string]any{{"field": "w", "operator": "eq", "value": w.id}}}}, &n); err != nil {
			t.Fatalf("count_documents w=%d: %v", w.id, err)
		}
		if n.Count != perWriter[float64(w.id)] {
			t.Errorf("writer %d: indexed count %d, scan %d", w.id, n.Count, perWriter[float64(w.id)])
		}
		w.live = w.live[:0]
		for id, st := range w.docs {
			got, present := stored[id]
			switch {
			case reflect.DeepEqual(got, st.acked) && present == (st.acked != nil):
				// the acknowledged version
				if st.hasPending && st.batch != "" {
					landed[st.batch] = append(landed[st.batch], false)
				}
			case st.hasPending && reflect.DeepEqual(got, st.pending) && present == (st.pending != nil):
				st.acked = st.pending // the in-flight write landed
				if st.batch != "" {
					landed[st.batch] = append(landed[st.batch], true)
				}
			case st.acked == nil && st.hasPending:
				t.Errorf("document %s: stored %v, want absent or the in-flight %v", id, got, st.pending)
			case st.acked == nil:
				t.Errorf("document %s: deleted (acknowledged) but stored as %v", id, got)
			default:
				t.Errorf("LOST ACKNOWLEDGED WRITE: document %s: stored %v (present %v), want %v (or in-flight %v)", id, got, present, st.acked, st.pending)
			}
			st.hasPending, st.pending, st.batch = false, nil, ""
			if st.acked != nil {
				w.live = append(w.live, id)
			}
		}
		slices.Sort(w.live) // map order is random; keep the next round reproducible from its seed

		var ctr findOutput
		if err := c.call(ctx, "find_documents", map[string]any{"collection": countersColl,
			"query": map[string]any{"filters": []map[string]any{{"field": "_id", "operator": "eq", "value": fmt.Sprintf("w%d", w.id)}}}}, &ctr); err != nil {
			t.Fatalf("reading counter %d: %v", w.id, err)
		}
		if len(ctr.Documents) != 1 {
			t.Fatalf("counter w%d: %d documents", w.id, len(ctr.Documents))
		}
		n2, _ := ctr.Documents[0]["n"].(float64)
		switch {
		case n2 == w.counter:
		case n2 == w.counter+1 && w.counterPending:
			w.counter = n2 // the in-flight increment landed
		default:
			t.Errorf("counter w%d = %v, acknowledged %v (in flight: %v)", w.id, n2, w.counter, w.counterPending)
		}
		w.counterPending = false
	}
	for batch, l := range landed {
		if slices.Contains(l, true) && slices.Contains(l, false) {
			t.Errorf("BATCH NOT ATOMIC: %s: of its in-flight writes, some landed and some did not: %v", batch, l)
		}
	}
	for id := range stored {
		known := false
		for _, w := range ws {
			if _, ok := w.docs[id]; ok {
				known = true
			}
		}
		if !known {
			t.Errorf("document %s is stored but was never written", id)
		}
	}
}

// ---- the test ----

// TestKillRecovery runs the cachydb binary over HTTP with several
// concurrent writers, kills it with SIGKILL at a random moment (every
// 5th round: a clean SIGTERM), restarts it on the same data directory,
// and checks that every acknowledged write is there, every acknowledged
// delete is gone, every counter is at least its acknowledged value, every
// collection opens and counts like a scan, and no ERROR was logged. Data
// carries over between rounds, so recovery runs on top of recovery.
//
// Not covered: tiny data segments or flush intervals (the binary has no
// flag or environment variable for them; the 1s default flush interval
// still lets kills land during flushes).
func TestKillRecovery(t *testing.T) {
	rounds := 10
	if s := os.Getenv("CACHYDB_E2E_ROUNDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			t.Fatalf("CACHYDB_E2E_ROUNDS=%q: want a positive integer", s)
		}
		rounds = n
	}
	seed := uint64(time.Now().UnixNano())
	t.Logf("%d rounds, seed %d, data under %s", rounds, seed, os.TempDir())
	rng := mrand.New(mrand.NewPCG(seed, seed))

	bin := buildBinary(t)
	base := t.TempDir()
	dir := filepath.Join(base, "data")
	logs := filepath.Join(base, "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	keepArtifacts(t, dir, logs)
	ctx := context.Background()

	ws := make([]*writerState, writers)
	for i := range ws {
		ws[i] = newWriterState(i)
	}

	// Setup, acknowledged before the first round.
	srv := startServer(t, bin, dir, logs, 0, freePort(t))
	func() {
		c, err := connect(ctx, srv.url)
		if err != nil {
			t.Fatal(err)
		}
		defer c.close()
		for _, coll := range []string{docsColl, countersColl} {
			if err := c.call(ctx, "create_collection", map[string]any{"name": coll}, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.call(ctx, "create_index", map[string]any{"collection": docsColl, "index_name": "by_w", "field_name": "w"}, nil); err != nil {
			t.Fatal(err)
		}
		for i := range ws {
			if err := c.call(ctx, "insert_document", map[string]any{"collection": countersColl, "document": map[string]any{"_id": fmt.Sprintf("w%d", i), "n": 0}}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}()

	cleanStops, kills, replayedAfterKill := 0, 0, 0
	prevClean := false
	for round := 1; round <= rounds && !t.Failed(); round++ {
		if round > 1 {
			srv = startServer(t, bin, dir, logs, round, freePort(t))
			replayed := strings.Contains(readFile(t, srv.log), "recovered changes from the WAL")
			if prevClean && replayed {
				t.Errorf("round %d: the WAL was replayed after a clean SIGTERM shutdown", round)
			}
			if !prevClean && replayed {
				replayedAfterKill++
			}
			verify(t, ctx, srv.url, ws)
			if t.Failed() {
				break
			}
		}

		// Write concurrently, then stop the server at a random moment.
		var stopped atomic.Bool
		wctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for _, w := range ws {
			wg.Add(1)
			wrng := mrand.New(mrand.NewPCG(rng.Uint64(), rng.Uint64()))
			go func() {
				defer wg.Done()
				if err := w.run(wctx, srv.url, round, wrng, &stopped); err != nil {
					errs <- err
				}
			}()
		}
		time.Sleep(time.Duration(50+rng.IntN(751)) * time.Millisecond)
		clean := round%cleanStopEach == 0
		stopped.Store(true)
		if clean {
			srv.stop(t, syscall.SIGTERM)
			if srv.waitErr != nil {
				t.Errorf("round %d: clean stop exited with %v; log:\n%s", round, srv.waitErr, readFile(t, srv.log))
			}
			cleanStops++
		} else {
			srv.stop(t, syscall.SIGKILL)
			kills++
		}
		cancel()
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("round %d: %v", round, err)
		}
		checkLog(t, srv.log)
		prevClean = clean
	}

	// Final restart and check, then a clean stop.
	if !t.Failed() {
		srv = startServer(t, bin, dir, logs, rounds+1, freePort(t))
		verify(t, ctx, srv.url, ws)
		srv.stop(t, syscall.SIGTERM)
		if srv.waitErr != nil {
			t.Errorf("final clean stop exited with %v", srv.waitErr)
		}
		checkLog(t, srv.log)
	}

	total := map[string]int{}
	live := 0
	for _, w := range ws {
		for k, v := range w.ops {
			total[k] += v
		}
		live += len(w.live)
	}
	t.Logf("%d kills (%d recovered WAL changes on restart), %d clean stops; acknowledged operations %v; %d live documents at the end",
		kills, replayedAfterKill, cleanStops, total, live)
	var b bytes.Buffer
	for _, w := range ws {
		fmt.Fprintf(&b, " w%d=%v", w.id, w.counter)
	}
	t.Logf("counters:%s", b.String())
}

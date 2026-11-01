package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/hop-/cachydb/internal/docs"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

func TestPrintDocs(t *testing.T) {
	topics, err := mcpserver.DocsTopics()
	if err != nil {
		t.Fatalf("DocsTopics: %v", err)
	}

	t.Run("index", func(t *testing.T) {
		var out bytes.Buffer
		if err := printDocs(&out, "", false, "md"); err != nil {
			t.Fatal(err)
		}
		for _, tp := range topics {
			found := false
			for _, line := range strings.Split(out.String(), "\n") {
				f := strings.Fields(line)
				if len(f) > 2 && f[0] == tp.Name && f[1] == "—" && strings.HasSuffix(line, "— "+tp.Summary) {
					found = true
				}
			}
			if !found {
				t.Errorf("index has no line for %s:\n%s", tp.Name, out.String())
			}
		}
	})
	t.Run("index json", func(t *testing.T) {
		var out bytes.Buffer
		if err := printDocs(&out, "", false, "json"); err != nil {
			t.Fatal(err)
		}
		var idx []map[string]string
		if err := json.Unmarshal(out.Bytes(), &idx); err != nil {
			t.Fatalf("index JSON: %v\n%s", err, out.String())
		}
		if len(idx) != len(docs.Names) {
			t.Fatalf("index has %d entries, want %d", len(idx), len(docs.Names))
		}
		for i, e := range idx {
			if e["name"] != docs.Names[i] || e["title"] == "" || e["summary"] == "" || len(e) != 3 {
				t.Errorf("entry %d = %v", i, e)
			}
		}
	})
	for _, tp := range topics {
		t.Run("topic "+tp.Name, func(t *testing.T) {
			var md bytes.Buffer
			if err := printDocs(&md, tp.Name, false, "md"); err != nil {
				t.Fatal(err)
			}
			if md.String() != tp.Content {
				t.Errorf("md output differs from the topic content")
			}
			var js bytes.Buffer
			if err := printDocs(&js, tp.Name, false, "json"); err != nil {
				t.Fatal(err)
			}
			var got struct {
				Name, Title, Summary, Content string
				Tools                         []json.RawMessage
			}
			if err := json.Unmarshal(js.Bytes(), &got); err != nil {
				t.Fatalf("topic JSON: %v", err)
			}
			if got.Name != tp.Name || got.Title != tp.Title || got.Summary != tp.Summary || got.Content != tp.Content {
				t.Errorf("JSON topic = %+v", got)
			}
			if (tp.Name == docs.ToolsTopic) != (len(got.Tools) > 0) {
				t.Errorf("tools field present = %v for topic %s", len(got.Tools) > 0, tp.Name)
			}
		})
	}
	t.Run("all", func(t *testing.T) {
		var out bytes.Buffer
		if err := printDocs(&out, "", true, "md"); err != nil {
			t.Fatal(err)
		}
		last := -1
		for _, tp := range topics {
			i := strings.Index(out.String(), tp.Content)
			if i < 0 || i < last {
				t.Fatalf("--all does not contain topic %s in order", tp.Name)
			}
			last = i
		}
	})
	t.Run("all json", func(t *testing.T) {
		var out bytes.Buffer
		if err := printDocs(&out, "", true, "json"); err != nil {
			t.Fatal(err)
		}
		var all []map[string]any
		if err := json.Unmarshal(out.Bytes(), &all); err != nil || len(all) != len(docs.Names) {
			t.Fatalf("--all --format json: %v, %d topics", err, len(all))
		}
	})
	errCases := []struct {
		name, topic, format string
		all                 bool
		wantErr             string
	}{
		{name: "unknown topic", topic: "nope", format: "md", wantErr: "unknown docs topic"},
		{name: "bad format", format: "yaml", wantErr: "invalid --format"},
		{name: "topic and all", topic: "query", all: true, format: "md", wantErr: "not both"},
	}
	for _, tt := range errCases {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := printDocs(&out, tt.topic, tt.all, tt.format)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
			if out.Len() != 0 {
				t.Errorf("wrote %d bytes to stdout on error", out.Len())
			}
		})
	}
	t.Run("unknown topic lists topics", func(t *testing.T) {
		err := printDocs(&bytes.Buffer{}, "nope", false, "md")
		if !errors.Is(err, docs.ErrUnknownTopic) || !strings.Contains(err.Error(), strings.Join(docs.Names, ", ")) {
			t.Fatalf("err = %v, want ErrUnknownTopic listing the topics", err)
		}
	})
}

// TestDocsToolsTopicMatchesDefinitions: the tools topic in JSON lists
// exactly ToolDefinitions(), with their schemas and annotations.
func TestDocsToolsTopicMatchesDefinitions(t *testing.T) {
	var out bytes.Buffer
	if err := printDocs(&out, docs.ToolsTopic, false, "json"); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Tools []struct {
			Name         string         `json:"name"`
			Description  string         `json:"description"`
			Annotations  map[string]any `json:"annotations"`
			InputSchema  map[string]any `json:"inputSchema"`
			OutputSchema map[string]any `json:"outputSchema"`
			Example      map[string]any `json:"example"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	defs := mcpserver.ToolDefinitions()
	if len(got.Tools) != len(defs) {
		t.Fatalf("tools topic lists %d tools, ToolDefinitions %d", len(got.Tools), len(defs))
	}
	for i, d := range defs {
		g := got.Tools[i]
		if g.Name != d.Name || g.Description != d.Description {
			t.Errorf("tool %d = %s, want %s", i, g.Name, d.Name)
		}
		if g.Annotations == nil || g.InputSchema == nil || g.OutputSchema == nil || g.Example == nil {
			t.Errorf("tool %s lacks annotations, schemas, or example", g.Name)
		}
	}
}

func TestVersionJSON(t *testing.T) {
	var out bytes.Buffer
	if err := showVersionJSON(&out); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("version JSON: %v", err)
	}
	for _, k := range []string{"version", "go", "storageFormat", "walFormat", "mcpProtocol"} {
		if _, ok := got[k]; !ok {
			t.Errorf("no %q in %v", k, got)
		}
	}
	if len(got) != 5 {
		t.Errorf("version JSON has %d keys, want 5: %v", len(got), got)
	}
	if got["version"] != getVersion() || got["storageFormat"] != float64(db.FormatVersion) || got["walFormat"] != float64(db.WALFormatVersion) {
		t.Errorf("version JSON = %v", got)
	}
}

// allCommands returns rootCmd and every subcommand except cobra's own
// (help, completion).
func allCommands() []*cobra.Command {
	var out []*cobra.Command
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		if c.Name() == "help" || c.Name() == "completion" {
			return
		}
		out = append(out, c)
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(rootCmd)
	return out
}

func TestHelpExamples(t *testing.T) {
	for _, c := range allCommands() {
		t.Run(c.CommandPath(), func(t *testing.T) {
			if n := strings.Count(strings.TrimSpace(c.Example), "\n") + 1; c.Example == "" || n < 2 {
				t.Errorf("Example has %d lines, want at least 2", n)
			}
			for _, ref := range docs.References(c.Long + "\n" + c.Example) {
				if _, err := docs.Get(ref); err != nil {
					t.Errorf("help references unknown docs topic %q", ref)
				}
			}
		})
	}
}

func TestDBFlag(t *testing.T) {
	for _, c := range []*cobra.Command{rootCmd, appCmd} {
		f := c.Flags().Lookup("db")
		if f == nil || f.Shorthand != "d" || !strings.Contains(f.Usage, "DB_NAME") {
			t.Errorf("%s: --db flag = %+v", c.Name(), f)
		}
	}
}

package docs

import (
	"errors"
	"strings"
	"testing"
)

func TestTopics(t *testing.T) {
	topics, err := All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(topics) != len(Names) {
		t.Fatalf("All returned %d topics, want %d", len(topics), len(Names))
	}
	for i, topic := range topics {
		t.Run(topic.Name, func(t *testing.T) {
			if topic.Name != Names[i] {
				t.Errorf("topic %d = %q, want %q", i, topic.Name, Names[i])
			}
			if topic.Title == "" || topic.Summary == "" {
				t.Errorf("empty title or summary: %+v", topic)
			}
			if strings.Contains(topic.Summary, "\n") {
				t.Errorf("summary is not one line: %q", topic.Summary)
			}
			if !strings.HasPrefix(topic.Content, "# ") {
				t.Errorf("content does not start with a heading: %.60q", topic.Content)
			}
			if strings.Contains(topic.Content, "<!--") {
				t.Errorf("content still has an HTML comment")
			}
		})
	}
}

// TestTopicReferences checks that every `cachydb docs <topic>` and
// cachydb://docs/<topic> mentioned in a topic names an existing topic.
func TestTopicReferences(t *testing.T) {
	topics, err := All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	for _, topic := range topics {
		for _, ref := range References(topic.Content) {
			if _, err := Get(ref); err != nil {
				t.Errorf("topic %s references unknown topic %q", topic.Name, ref)
			}
		}
	}
}

func TestGetUnknown(t *testing.T) {
	_, err := Get("nope")
	if !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("Get(nope) err = %v, want ErrUnknownTopic", err)
	}
	if !strings.Contains(err.Error(), "overview") {
		t.Errorf("error %q does not list the topics", err)
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    Topic
		wantErr string
	}{
		{
			name: "valid",
			raw:  "---\nname: a\ntitle: A title\nsummary: One: line\n---\n\n<!-- note -->\n# A\nbody\n",
			want: Topic{Name: "a", Title: "A title", Summary: "One: line", Content: "# A\nbody\n"},
		},
		{name: "no front matter", raw: "# A\n", wantErr: "missing front matter"},
		{name: "unterminated", raw: "---\nname: a\n", wantErr: "unterminated"},
		{name: "unknown key", raw: "---\nname: a\ntitle: t\nsummary: s\nauthor: x\n---\n# A\n", wantErr: "unknown key"},
		{name: "bad line", raw: "---\nname a\n---\n# A\n", wantErr: "want key: value"},
		{name: "missing summary", raw: "---\nname: a\ntitle: t\n---\n# A\n", wantErr: "needs name, title, and summary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parse(tt.raw)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parse err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parse = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestIndex(t *testing.T) {
	got := Index([]Topic{{Name: "a", Summary: "first"}, {Name: "long", Summary: "second"}})
	want := "  a    — first\n  long — second\n"
	if got != want {
		t.Fatalf("Index = %q, want %q", got, want)
	}
}

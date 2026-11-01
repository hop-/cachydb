// Package docs holds cachydb's built-in, agent-facing documentation: the
// topics printed by `cachydb docs` and served as cachydb://docs/<topic>
// MCP resources. The topics are markdown files with front matter, embedded
// in the binary. The "tools" topic file only holds the front matter and an
// introduction; internal/mcp appends the generated tool reference.
package docs

import (
	"bufio"
	"embed"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

//go:embed topics/*.md
var topicFS embed.FS

// Names lists every topic in display order.
var Names = []string{"overview", "quickstart", "mcp", "tools", "query", "schema", "config", "errors"}

// ToolsTopic is the name of the generated topic.
const ToolsTopic = "tools"

// Topic is one documentation topic.
type Topic struct {
	Name    string `json:"name"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Content string `json:"content"` // markdown, without the front matter
}

// ErrUnknownTopic is returned (wrapped) by Get for a name not in Names.
var ErrUnknownTopic = errors.New("unknown docs topic")

// Get returns the embedded topic name. For the tools topic it returns only
// the static part (front matter and introduction).
func Get(name string) (Topic, error) {
	if !known(name) {
		return Topic{}, fmt.Errorf("%w %q (topics: %s)", ErrUnknownTopic, name, strings.Join(Names, ", "))
	}
	raw, err := topicFS.ReadFile("topics/" + name + ".md")
	if err != nil {
		return Topic{}, fmt.Errorf("reading docs topic %q: %w", name, err)
	}
	t, err := parse(string(raw))
	if err != nil {
		return Topic{}, fmt.Errorf("parsing docs topic %q: %w", name, err)
	}
	if t.Name != name {
		return Topic{}, fmt.Errorf("parsing docs topic %q: front matter name is %q", name, t.Name)
	}
	return t, nil
}

// All returns every topic in display order (the tools topic static only).
func All() ([]Topic, error) {
	out := make([]Topic, 0, len(Names))
	for _, n := range Names {
		t, err := Get(n)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

func known(name string) bool {
	for _, n := range Names {
		if n == name {
			return true
		}
	}
	return false
}

// parse splits a topic file into its front matter (name, title, summary;
// one "key: value" per line between "---" lines) and its content.
func parse(raw string) (Topic, error) {
	rest, ok := strings.CutPrefix(raw, "---\n")
	if !ok {
		return Topic{}, errors.New("missing front matter")
	}
	header, content, ok := strings.Cut(rest, "\n---\n")
	if !ok {
		return Topic{}, errors.New("unterminated front matter")
	}
	var t Topic
	sc := bufio.NewScanner(strings.NewReader(header))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			return Topic{}, fmt.Errorf("front matter line %q: want key: value", line)
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(key) {
		case "name":
			t.Name = value
		case "title":
			t.Title = value
		case "summary":
			t.Summary = value
		default:
			return Topic{}, fmt.Errorf("front matter: unknown key %q", key)
		}
	}
	if err := sc.Err(); err != nil {
		return Topic{}, fmt.Errorf("reading front matter: %w", err)
	}
	if t.Name == "" || t.Title == "" || t.Summary == "" {
		return Topic{}, errors.New("front matter needs name, title, and summary")
	}
	t.Content = stripLeadingComments(content)
	return t, nil
}

// refPattern matches a docs topic reference: "cachydb docs <topic>" or
// "cachydb://docs/<topic>".
var refPattern = regexp.MustCompile(`cachydb(?: docs |://docs/)([a-z]+)`)

// References returns the topic names text refers to with "cachydb docs
// <topic>" or "cachydb://docs/<topic>". Help text, instructions, and tests
// use it to check that every reference names an existing topic.
func References(text string) []string {
	var out []string
	for _, m := range refPattern.FindAllStringSubmatch(text, -1) {
		out = append(out, m[1])
	}
	return out
}

// stripLeadingComments drops blank lines and HTML comments (maintainer
// notes such as source URLs) from the start of content.
func stripLeadingComments(content string) string {
	for {
		content = strings.TrimLeft(content, "\n")
		rest, ok := strings.CutPrefix(content, "<!--")
		if !ok {
			return content
		}
		_, after, ok := strings.Cut(rest, "-->")
		if !ok {
			return content
		}
		content = after
	}
}

// Index renders the topic list, one "name — summary" line per topic.
func Index(topics []Topic) string {
	var b strings.Builder
	width := 0
	for _, t := range topics {
		width = max(width, len(t.Name))
	}
	for _, t := range topics {
		fmt.Fprintf(&b, "  %-*s — %s\n", width, t.Name, t.Summary)
	}
	return b.String()
}

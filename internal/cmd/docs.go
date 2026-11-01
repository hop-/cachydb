package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/hop-/cachydb/internal/docs"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/spf13/cobra"
)

var docsCmd = &cobra.Command{
	Use:   "docs [topic]",
	Short: "Print the built-in documentation for AI agents and humans",
	Long: `Print cachydb's built-in documentation. Without a topic it lists the
topics; with a topic it prints that topic as markdown.

Topics: ` + strings.Join(docs.Names, ", ") + `.

The tools topic is generated from the MCP tool definitions of this binary,
so it always matches what the server registers. The same topics are served
over MCP as resources cachydb://docs/<topic>.

docs never opens the database, so it works while a server is running.`,
	Example: `  cachydb docs                         # list the topics
  cachydb docs quickstart              # print one topic (markdown)
  cachydb docs tools --format json     # tool schemas as JSON
  cachydb docs --all                   # every topic, in order`,
	Args:      cobra.MaximumNArgs(1),
	ValidArgs: docs.Names,
	RunE:      runDocs,
}

var (
	docsAll    bool
	docsFormat string
)

func init() {
	docsCmd.Flags().BoolVarP(&docsAll, "all", "a", false, "print every topic, in order")
	docsCmd.Flags().StringVarP(&docsFormat, "format", "f", "md", "output format: md or json")
	rootCmd.AddCommand(docsCmd)
}

func runDocs(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	topic := ""
	if len(args) == 1 {
		topic = args[0]
	}
	return printDocs(cmd.OutOrStdout(), topic, docsAll, docsFormat)
}

// topicJSON is a topic in --format json; Tools is set for the tools topic.
type topicJSON struct {
	docs.Topic
	Tools []mcpserver.ToolDoc `json:"tools,omitempty"`
}

// indexEntry is one topic of the JSON index.
type indexEntry struct {
	Name    string `json:"name"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

// printDocs writes the index (no topic, not all), one topic, or every
// topic to w in format "md" or "json". Nothing is written on error.
func printDocs(w io.Writer, topic string, all bool, format string) error {
	if format != "md" && format != "json" {
		return fmt.Errorf("invalid --format %q: use md or json", format)
	}
	if all && topic != "" {
		return fmt.Errorf("give a topic or --all, not both")
	}

	var out []byte
	switch {
	case topic != "":
		t, err := mcpserver.DocsTopic(topic)
		if err != nil {
			return err
		}
		out = renderTopic(t, format)
	default:
		topics, err := mcpserver.DocsTopics()
		if err != nil {
			return err
		}
		if all {
			out = renderAll(topics, format)
		} else {
			out = renderIndex(topics, format)
		}
	}
	_, err := w.Write(out)
	return err
}

func toJSON(v any) []byte {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		// Topics are strings and plain data; this cannot happen.
		return []byte(fmt.Sprintf("%q\n", err.Error()))
	}
	return append(data, '\n')
}

func asTopicJSON(t docs.Topic) topicJSON {
	tj := topicJSON{Topic: t}
	if t.Name == docs.ToolsTopic {
		tj.Tools = mcpserver.ToolDocs()
	}
	return tj
}

func renderTopic(t docs.Topic, format string) []byte {
	if format == "json" {
		return toJSON(asTopicJSON(t))
	}
	return []byte(t.Content)
}

func renderAll(topics []docs.Topic, format string) []byte {
	if format == "json" {
		all := make([]topicJSON, len(topics))
		for i, t := range topics {
			all[i] = asTopicJSON(t)
		}
		return toJSON(all)
	}
	var b strings.Builder
	for i, t := range topics {
		if i > 0 {
			b.WriteString("\n---\n\n")
		}
		b.WriteString(t.Content)
	}
	return []byte(b.String())
}

func renderIndex(topics []docs.Topic, format string) []byte {
	if format == "json" {
		idx := make([]indexEntry, len(topics))
		for i, t := range topics {
			idx[i] = indexEntry{Name: t.Name, Title: t.Title, Summary: t.Summary}
		}
		return toJSON(idx)
	}
	return []byte("CachyDB documentation topics:\n\n" + docs.Index(topics) +
		"\nRun 'cachydb docs <topic>' to read one, or 'cachydb docs --all' for everything.\n" +
		"Over MCP, read the resource cachydb://docs/<topic>.\n")
}

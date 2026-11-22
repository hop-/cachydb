package mcpserver

import (
	"fmt"
	"strings"
)

// summarizer is implemented by every tool output: its one-line summary is
// the text content of a successful result in ResultTextSummary mode
// (approach-agent-interface §8). The structured content stays complete,
// so a summary only names counts and identifiers, never document data.
type summarizer interface {
	summary() string
}

// summaryOf returns out's summary line, or "ok" for an output without one.
func summaryOf(out any) string {
	if s, ok := out.(summarizer); ok {
		return s.summary()
	}
	return "ok"
}

// plural returns "<n> <word>", adding "s" to word unless n is 1.
func plural(n int, word string) string { return pluralForm(n, word, word+"s") }

// pluralForm returns "<n> <one>" for n == 1, else "<n> <many>".
func pluralForm(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

func (o MessageOutput) summary() string         { return o.Message }
func (o UseDatabaseOutput) summary() string     { return o.Message }
func (o InsertDocumentOutput) summary() string  { return o.Message }
func (o UpdateDocumentOutput) summary() string  { return o.Message }
func (o CopyCollectionOutput) summary() string  { return o.Message }
func (o SetAccessOutput) summary() string       { return o.Message }
func (o ListDatabasesOutput) summary() string   { return plural(len(o.Databases), "database") }
func (o CountDocumentsOutput) summary() string  { return plural(o.Count, "document") }
func (o DistinctOutput) summary() string        { return plural(len(o.Values), "distinct value") }
func (o InsertDocumentsOutput) summary() string { return plural(o.Inserted, "document") + " inserted" }
func (o ListUsersOutput) summary() string       { return plural(len(o.Users), "user") }

func (o CurrentDatabaseOutput) summary() string {
	return "current database: " + o.CurrentDatabase
}

func (o ListCollectionsOutput) summary() string {
	return fmt.Sprintf("%s in database %s", plural(len(o.Collections), "collection"), o.Database)
}

func (o FindDocumentsOutput) summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s, has_more: %t", plural(o.Count, "document"), o.HasMore)
	if o.HasMore {
		fmt.Fprintf(&b, ", next_skip: %d", o.NextSkip)
	}
	if o.Plan != nil {
		fmt.Fprintf(&b, ", plan: %s", o.Plan.Driver)
		if o.Plan.Index != "" {
			fmt.Fprintf(&b, " %s", o.Plan.Index)
		}
		fmt.Fprintf(&b, ", examined: %d", o.Plan.Examined)
	}
	return b.String()
}

func (o GetDocumentOutput) summary() string {
	return fmt.Sprintf("1 document, _id: %v", o.Document["_id"])
}

func (o UpdateDocumentsOutput) summary() string {
	return fmt.Sprintf("%d matched, %d modified", o.Matched, o.Modified)
}

func (o DeleteDocumentsOutput) summary() string {
	return fmt.Sprintf("%d matched, %d deleted", o.Matched, o.Deleted)
}

func (o DescribeCollectionOutput) summary() string {
	schema := "none"
	if o.Schema != nil {
		schema = plural(len(o.Schema.Fields), "field")
	}
	return fmt.Sprintf("collection %s in database %s: %s, %s, schema: %s",
		o.Name, o.Database, plural(o.Count, "document"), pluralForm(len(o.Indexes), "index", "indexes"), schema)
}

func (o ServerStatsOutput) summary() string {
	var colls int
	var docs int64
	for _, d := range o.Databases {
		colls += len(d.Collections)
		docs += d.Documents
	}
	return fmt.Sprintf("%s, %s, %d documents", plural(len(o.Databases), "database"), plural(colls, "collection"), docs)
}

func (o WhoamiOutput) summary() string {
	return fmt.Sprintf("user %s, server_admin: %t, %s", o.User, o.ServerAdmin, plural(len(o.Grants), "grant"))
}

// The user tools that create a token keep it in the summary: it is shown
// only once, and a client that gives its model only the text content
// would otherwise lose it.

func (o CreateUserOutput) summary() string {
	return fmt.Sprintf("%s Token (label %s): %s", o.Message, o.Label, o.Token)
}

func (o CreateTokenOutput) summary() string {
	return fmt.Sprintf("%s Token: %s", o.Message, o.Token)
}

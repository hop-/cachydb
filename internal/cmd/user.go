package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/hop-/cachydb/internal/auth"
	mcpserver "github.com/hop-/cachydb/internal/mcp"
	"github.com/hop-/cachydb/pkg/db"
	"github.com/spf13/cobra"
)

// userActor is the actor the logs name for changes made by "cachydb utils
// user".
const userActor = "cachydb utils user"

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "Manage users, their access, and their tokens (offline)",
	Long: `Manage the users of a data directory while the server is stopped: create
and remove users, grant and revoke access to databases, and create and
revoke tokens.

A user has a name, an optional server-admin flag (every database, creating
and deleting databases, and managing users), an optional default database,
and per-database access levels: read, write (read plus document writes),
or admin (write plus collections, schemas, and indexes). Database * means
every database, including future ones.

A new token is printed exactly once, alone on stdout, so a script can
capture it; only its SHA-256 digest is stored, and "list" never shows
tokens. The only server admin cannot be removed.

These commands take the data-directory lock, so they fail while a server
runs on it; a running server's admins use the MCP tools list_users,
create_user, set_access, create_token, revoke_token, and delete_user
instead (for example with "cachydb client call"). See "cachydb docs access".`,
	Example: `  cachydb utils user add admin --admin
  cachydb utils user add alice --default-database shop
  cachydb utils user grant alice shop write
  cachydb utils user list -R /path/to/data`,
}

var userAddCmd = &cobra.Command{
	Use:   "add NAME",
	Short: "Create a user and print its first token once",
	Long: `Create a user and print its first token (label "default") exactly once,
alone on stdout; a note goes to stderr. Store the token now: it cannot be
shown again, only replaced with "user token add".

The name follows the database-name rule: letters, digits, _ and -,
starting with a letter or digit, at most 64 characters.`,
	Example: `  cachydb utils user add admin --admin
  TOKEN="$(cachydb utils user add alice --default-database shop -R /srv/cachydb)"`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return userAdd(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), engineOptions(), args[0],
			auth.UserOptions{ServerAdmin: userAddAdmin, DefaultDatabase: userAddDefaultDB})
	},
}

var userListCmd = &cobra.Command{
	Use:   "list",
	Short: "List users, their access, and their token labels",
	Long: `List every user with its server-admin flag, default database, grants, and
token labels. Tokens and their digests are never shown.`,
	Example: `  cachydb utils user list
  cachydb utils user list --format json`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if userListFormat != "text" && userListFormat != "json" {
			return fmt.Errorf("invalid --format %q: use text or json", userListFormat)
		}
		cmd.SilenceUsage = true
		return userList(cmd.Context(), cmd.OutOrStdout(), engineOptions(), userListFormat)
	},
}

var userRemoveCmd = &cobra.Command{
	Use:   "remove NAME",
	Short: "Remove a user and all its tokens",
	Long: `Remove a user and all its tokens. The only server admin cannot be removed:
make another user a server admin first.`,
	Example: `  cachydb utils user remove alice
  cachydb utils user remove alice -R /srv/cachydb`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return userChange(cmd.Context(), cmd.ErrOrStderr(), engineOptions(), func(ctx context.Context, s *auth.Store, logger *slog.Logger) (string, error) {
			if err := s.DeleteUser(ctx, args[0]); err != nil {
				return "", err
			}
			auth.LogChange(logger, userActor, "delete_user", args[0])
			return fmt.Sprintf("User %q and its tokens removed.", args[0]), nil
		})
	},
}

var userGrantCmd = &cobra.Command{
	Use:   "grant NAME DATABASE read|write|admin",
	Short: "Set a user's access level on a database",
	Long: `Set the user's access level on DATABASE, replacing an earlier level there:
read, write (read plus document writes), or admin (write plus collections,
schemas, and indexes). DATABASE * means every database, including future
ones; quote it in the shell. The database need not exist yet.`,
	Example: `  cachydb utils user grant alice shop write
  cachydb utils user grant reporter '*' read`,
	Args: cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		level, err := auth.ParseLevel(args[2])
		if err != nil {
			return err
		}
		cmd.SilenceUsage = true
		return userChange(cmd.Context(), cmd.ErrOrStderr(), engineOptions(), func(ctx context.Context, s *auth.Store, logger *slog.Logger) (string, error) {
			if err := s.Grant(ctx, args[0], args[1], level); err != nil {
				return "", err
			}
			auth.LogChange(logger, userActor, "set_access", args[0], "database", args[1], "level", level.String())
			return fmt.Sprintf("User %q now has %s access to %s.", args[0], level, databaseLabel(args[1])), nil
		})
	},
}

var userRevokeCmd = &cobra.Command{
	Use:   "revoke NAME DATABASE",
	Short: "Remove a user's access to a database",
	Long: `Remove the user's grant on DATABASE (* is the every-database grant). It
fails if the user has no grant there.`,
	Example: `  cachydb utils user revoke alice shop
  cachydb utils user revoke reporter '*'`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return userChange(cmd.Context(), cmd.ErrOrStderr(), engineOptions(), func(ctx context.Context, s *auth.Store, logger *slog.Logger) (string, error) {
			if err := s.Revoke(ctx, args[0], args[1]); err != nil {
				return "", err
			}
			auth.LogChange(logger, userActor, "set_access", args[0], "database", args[1], "level", auth.LevelNone.String())
			return fmt.Sprintf("User %q no longer has a grant on %s.", args[0], databaseLabel(args[1])), nil
		})
	},
}

var userTokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Create and revoke a user's tokens (offline)",
	Long: `Create and revoke a user's tokens. A user may hold several tokens, told
apart by labels; "cachydb utils user list" shows the labels.`,
	Example: `  cachydb utils user token add alice --label laptop
  cachydb utils user token revoke alice laptop`,
}

var userTokenAddCmd = &cobra.Command{
	Use:   "add NAME",
	Short: "Create a token for a user and print it once",
	Long: `Create a token for the user and print it exactly once, alone on stdout; a
note goes to stderr. Without --label the first free label of default,
default-2, ... is used. Labels follow the database-name rule and are
unique per user.`,
	Example: `  cachydb utils user token add alice --label laptop
  TOKEN="$(cachydb utils user token add ci-bot)"`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return userTokenAdd(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), engineOptions(), args[0], userTokenLabel)
	},
}

var userTokenRevokeCmd = &cobra.Command{
	Use:   "revoke NAME LABEL",
	Short: "Revoke a user's token by label",
	Long: `Revoke the user's token with the given label; it stops working the next
time a server starts on the data directory.`,
	Example: `  cachydb utils user token revoke alice laptop
  cachydb utils user token revoke admin bootstrap   # also unset BOOTSTRAP_ADMIN_TOKEN, or the next start adds it again`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return userChange(cmd.Context(), cmd.ErrOrStderr(), engineOptions(), func(ctx context.Context, s *auth.Store, logger *slog.Logger) (string, error) {
			if err := s.RevokeToken(ctx, args[0], args[1]); err != nil {
				return "", err
			}
			auth.LogChange(logger, userActor, "revoke_token", args[0], "label", args[1])
			return fmt.Sprintf("Token %q of user %q revoked.", args[1], args[0]), nil
		})
	},
}

var (
	userAddAdmin     bool
	userAddDefaultDB string
	userListFormat   string
	userTokenLabel   string
)

func init() {
	userAddCmd.Flags().BoolVar(&userAddAdmin, "admin", false, "make the user a server admin")
	userAddCmd.Flags().StringVar(&userAddDefaultDB, "default-database", "", "database the user's calls use when they name none")
	userListCmd.Flags().StringVarP(&userListFormat, "format", "f", "text", "output format: text or json")
	userTokenAddCmd.Flags().StringVar(&userTokenLabel, "label", "", "token label (default: default, default-2, ...)")
	userTokenCmd.AddCommand(userTokenAddCmd, userTokenRevokeCmd)
	userCmd.AddCommand(userAddCmd, userListCmd, userRemoveCmd, userGrantCmd, userRevokeCmd, userTokenCmd)
	utilsCmd.AddCommand(userCmd)
}

// withUsers opens the engine with opts and the auth store on it (creating
// the system database if it is missing), and calls fn.
func withUsers(ctx context.Context, opts db.Options, fn func(*auth.Store) error) error {
	return withEngine(ctx, opts, func(e *db.Engine) error {
		s, err := auth.Open(ctx, e)
		if err != nil {
			return err
		}
		return fn(s)
	})
}

// userChange runs one change and prints its summary on stderr.
func userChange(ctx context.Context, stderr io.Writer, opts db.Options, fn func(context.Context, *auth.Store, *slog.Logger) (string, error)) error {
	return withUsers(ctx, opts, func(s *auth.Store) error {
		msg, err := fn(ctx, s, opts.Logger)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stderr, msg)
		return err
	})
}

// databaseLabel names database in a message; * is every database.
func databaseLabel(database string) string {
	if database == auth.AllDatabases {
		return "every database (*)"
	}
	return fmt.Sprintf("database %q", database)
}

// userAdd creates the user name and prints its token alone on stdout.
func userAdd(ctx context.Context, stdout, stderr io.Writer, opts db.Options, name string, uo auth.UserOptions) error {
	return withUsers(ctx, opts, func(s *auth.Store) error {
		token, err := s.CreateUser(ctx, name, uo)
		if err != nil {
			return err
		}
		auth.LogChange(opts.Logger, userActor, "create_user", name, "server_admin", uo.ServerAdmin, "default_database", uo.DefaultDatabase)
		kind := "User"
		if uo.ServerAdmin {
			kind = "Server admin"
		}
		return printToken(stdout, stderr, token,
			fmt.Sprintf("%s %q created. Its token (label %q) follows on stdout and is shown only this once: store it now.", kind, name, auth.DefaultTokenLabel))
	})
}

// userTokenAdd creates a token for the user name and prints it alone on
// stdout.
func userTokenAdd(ctx context.Context, stdout, stderr io.Writer, opts db.Options, name, label string) error {
	return withUsers(ctx, opts, func(s *auth.Store) error {
		token, used, err := s.CreateToken(ctx, name, label)
		if err != nil {
			return err
		}
		auth.LogChange(opts.Logger, userActor, "create_token", name, "label", used)
		return printToken(stdout, stderr, token,
			fmt.Sprintf("Token %q of user %q created. It follows on stdout and is shown only this once: store it now.", used, name))
	})
}

// printToken prints note on stderr and token alone on stdout.
func printToken(stdout, stderr io.Writer, token, note string) error {
	if _, err := fmt.Fprintln(stderr, note); err != nil {
		return err
	}
	_, err := fmt.Fprintln(stdout, token)
	return err
}

// userList prints every user as a table or, with format "json", as a JSON
// array of the list_users tool's user objects.
func userList(ctx context.Context, w io.Writer, opts db.Options, format string) error {
	return withUsers(ctx, opts, func(s *auth.Store) error {
		users := s.Users()
		if format == "json" {
			out := make([]mcpserver.UserInfo, len(users))
			for i, u := range users {
				out[i] = mcpserver.NewUserInfo(u)
			}
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		}
		if len(users) == 0 {
			_, err := fmt.Fprintln(w, `No users. Create the first admin with "cachydb utils user add NAME --admin".`)
			return err
		}
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		out := &output{w: tw}
		out.printf("NAME\tSERVER ADMIN\tDEFAULT DATABASE\tGRANTS\tTOKENS\n")
		for _, u := range users {
			admin := "no"
			if u.ServerAdmin {
				admin = "yes"
			}
			out.printf("%s\t%s\t%s\t%s\t%s\n", u.Name, admin, orDash(u.DefaultDatabase), orDash(grantsText(u.Grants)), orDash(tokenLabels(u.Tokens)))
		}
		if out.err != nil {
			return out.err
		}
		return tw.Flush()
	})
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// grantsText renders grants as "db=level" pairs sorted by database.
func grantsText(grants map[string]auth.Level) string {
	parts := make([]string, 0, len(grants))
	for _, database := range slices.Sorted(maps.Keys(grants)) {
		parts = append(parts, database+"="+grants[database].String())
	}
	return strings.Join(parts, ",")
}

func tokenLabels(tokens []auth.Token) string {
	labels := make([]string, len(tokens))
	for i, t := range tokens {
		labels[i] = t.Label
	}
	return strings.Join(labels, ",")
}

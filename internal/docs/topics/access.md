---
name: access
title: Users, tokens, and access
summary: When tokens are needed, users and access levels, the first admin (utils user add, BOOTSTRAP_ADMIN_TOKEN), managing users offline and over MCP, whoami, cachydb client --token, and the user tools reference (generated)
---
# Users, tokens, and access

cachydb has users with tokens and per-database access levels. Whether a
client needs a token depends on the transport, the listen host, and
`AUTH` (see `cachydb docs config`): stdio and loopback HTTP need none
by default, HTTP that other machines can reach does. Without a token
requirement every client has full access and none of this applies.

## Access levels

| Level | Allows |
| --- | --- |
| `read` | `list_collections`, `describe_collection`, `find_documents`, `count_documents`, `distinct` |
| `write` | `read` plus `insert_document`, `insert_documents`, `update_document`, `update_documents`, `replace_document`, `delete_document`, `delete_documents` |
| `admin` | `write` plus `create_collection`, `update_schema`, `rename_collection`, `delete_collection`, `create_index`, `drop_index` |
| server admin | everything: every database, `create_database`, `delete_database`, and the user tools below |

A grant gives one level on one database; database `*` covers every
database, including ones created later, and the higher of the two
applies. `copy_collection` needs `read` on the source and `admin` on the
target database. `list_databases` shows only the databases the user can
read, and `use_database` needs `read`. Deleting a database removes every
grant on it, so a database created later under that name starts with
none. A call without the needed level fails with `permission denied`,
naming the user, the database, and the level (see `cachydb docs errors`).

A user may have a default database: calls that give no `database` and
whose session made no `use_database` choice use it before the server
default.

## The first admin

When clients need a token and no user exists, the server refuses to
start. Create the first server admin in one of two ways:

- **Offline**, while the server is stopped:
  `cachydb utils user add admin --admin -R /path/to/data` prints the
  token once, alone on stdout.
- **At start-up** (containers): set `BOOTSTRAP_ADMIN_TOKEN` to a token you
  choose, or `BOOTSTRAP_ADMIN_TOKEN_FILE` to a file holding one (for
  Docker secrets); not both. Every start then makes sure the user `admin`
  exists, is a server admin, and holds that token (label `bootstrap`).
  The value is never logged. It must be 32 to 512 characters of letters,
  digits, and `- . _ ~ + /` (optionally ending in `=`), for example the
  output of `openssl rand -hex 32`.

Changing `BOOTSTRAP_ADMIN_TOKEN` and restarting replaces the old
bootstrap token: only the current value works. Removing the setting
keeps the stored token; revoke it with
`cachydb utils user token revoke admin bootstrap` or `revoke_token`.
The setting is applied whenever it is set, also when no token is
required.

## Managing users offline

These commands take the data-directory lock (server stopped):

```bash
cachydb utils user add alice --default-database shop   # prints alice's token once
cachydb utils user grant alice shop write
cachydb utils user grant reporter '*' read             # every database
cachydb utils user revoke alice shop
cachydb utils user token add alice --label laptop      # prints the token once
cachydb utils user token revoke alice laptop
cachydb utils user list [--format json]                # never shows tokens
cachydb utils user remove alice
```

Tokens are `cdb_` plus 43 random characters; only their SHA-256 digest
is stored. The only server admin cannot be removed or demoted: make
another user a server admin first.

## Managing users over MCP

On a server that requires tokens, server admins also get the tools
`list_users`, `create_user`, `set_access`, `create_token`,
`revoke_token`, and `delete_user`; other users do not see them in
`tools/list`. Every user gets `whoami`. A server with authentication off
registers none of these. Changes apply at once, also to open sessions,
and are logged at INFO (who, what, which user; never a token). From a
shell, call them with `cachydb client call`:

```bash
export CACHYDB_URL=https://db.example:7601/mcp CACHYDB_TOKEN=<admin token>
cachydb client call create_user '{"name":"alice","default_database":"shop"}'
cachydb client call set_access '{"user":"alice","database":"shop","level":"write"}'
cachydb client call set_access '{"user":"alice","server_admin":true}'
cachydb client call whoami
```

## Clients

MCP clients send `Authorization: Bearer <token>` (in Claude Code:
`claude mcp add --transport http cachydb <url> --header "Authorization: Bearer <token>"`).
`cachydb client` takes `--token` or `CACHYDB_TOKEN` (preferred: a flag
shows in the process list); after a 401 answer its error names the flag.
It sends the token only to the scheme and host of `--url` and does not
follow redirects, so point `--url` at the final endpoint.
Without a valid token `/mcp` answers 401; `/healthz` never needs one. A
session belongs to the user that opened it: another user's token on it
gets 403. Use HTTPS (`--tls-cert`, `--tls-key`) or a trusted network, so
tokens are not sent in clear text.

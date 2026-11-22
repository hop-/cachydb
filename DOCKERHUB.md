# CachyDB

A lightweight document database with a built-in
[MCP (Model Context Protocol)](https://modelcontextprotocol.io) server,
written in Go. Databases hold collections of JSON documents, with optional
schemas and hash indexes. Every acknowledged write is fsynced to a
write-ahead log first, so it survives a crash. AI agents use it through 25
MCP tools (create collections, insert, find, update, delete, index), with
users, tokens, and per-database access when it serves other machines.

- Source and full documentation: <https://github.com/hop-/cachydb>
- Small static image: distroless, no shell, runs as the non-root user
  `65532`
- Platforms: `linux/amd64`, `linux/arm64`

## Quick start

### HTTP (a long-running server)

```bash
# Tokens required: the server admin "admin" holds the token you choose
TOKEN="$(openssl rand -hex 32)"; echo "$TOKEN"   # keep it
docker run -d --name cachydb -p 7601:7601 -v cachydb-data:/data \
  -e BOOTSTRAP_ADMIN_TOKEN="$TOKEN" hovhannespalyan/cachydb

# Local use without tokens: localhost only
docker run -d --name cachydb -p 127.0.0.1:7601:7601 -v cachydb-data:/data \
  -e AUTH=off hovhannespalyan/cachydb
```

The MCP endpoint is `http://localhost:7601/mcp` (Streamable HTTP). Many
clients can connect at once; with tokens they send
`Authorization: Bearer <token>`. Data lives in the `cachydb-data` volume
and survives container restarts and upgrades.

**A container needs a user or `AUTH=off`.** The image listens on every
interface of the container (`HOST=0.0.0.0`), so by default (`AUTH=auto`)
clients need a token, and the container exits at start with *"refusing
to start: ... no user exists"* until a user exists. Set
`BOOTSTRAP_ADMIN_TOKEN` (or `BOOTSTRAP_ADMIN_TOKEN_FILE`, a mounted file
or Docker secret holding it): every start makes sure the server admin
`admin` holds that token, and a changed value replaces the old one. Or
create users with `cachydb utils user add NAME --admin` on the stopped
volume. Or set `AUTH=off`, which is only safe when the port is published
on localhost or a trusted network (cachydb logs a warning). Use TLS
(`TLS_CERT`, `TLS_KEY`) when clients on other machines connect.
`/healthz` never needs a token.

Server admins then manage users over MCP (`create_user`, `set_access`,
`create_token`, ...; other users do not see these tools), for example
with the image's own client:

```bash
docker exec -e CACHYDB_TOKEN="$TOKEN" cachydb cachydb client call create_user '{"name":"alice"}'
docker exec -e CACHYDB_TOKEN="$TOKEN" cachydb cachydb client call set_access '{"user":"alice","database":"main","level":"write"}'
```

### stdio (the agent starts the container)

```bash
docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb
```

The MCP client starts this command and talks over stdin/stdout. The
container exits when the client closes stdin. Keep `-i` and leave out
`-t`: a TTY would corrupt the MCP messages.

## Supported tags

| Tag | Meaning |
| --- | --- |
| `latest` | The newest release |
| `X.Y` (e.g. `0.0`) | The newest patch release of `X.Y` |
| `X.Y.Z` (e.g. `0.0.4`) | One exact release; never changes |

Pin `X.Y.Z` in production. Each image carries SBOM and provenance
attestations (`docker buildx imagetools inspect hovhannespalyan/cachydb:<tag>`
shows them).

## Configuration

Configure the container with environment variables:

| Variable | Image default | Meaning |
| --- | --- | --- |
| `TRANSPORT` | `http` | `http` or `stdio` |
| `HOST` | `0.0.0.0` | HTTP listen host inside the container; every interface, so clients need a token unless `AUTH=off` |
| `PORT` | `7601` | HTTP port inside the container (path `/mcp`) |
| `AUTH` | `auto` | `auto`: tokens for HTTP on a non-loopback host (the image's case); `required`: always for HTTP; `off`: never (logs a warning on a non-loopback host) |
| `TLS_CERT`, `TLS_KEY` | none | PEM certificate and key files (mount them); with both, the server serves HTTPS. One without the other is an error |
| `BOOTSTRAP_ADMIN_TOKEN` | none | At every start, make sure the server admin `admin` exists and holds this token (32 to 512 characters, e.g. `openssl rand -hex 32`); never logged |
| `BOOTSTRAP_ADMIN_TOKEN_FILE` | none | A file holding the bootstrap token (e.g. `/run/secrets/...`); not together with `BOOTSTRAP_ADMIN_TOKEN` |
| `ROOT_DIR` | `/data` | Data directory. Keep it at `/data`, the volume |
| `DB_NAME` | `main` | Default database, created at startup if missing |
| `WAL_RETENTION_AGE` | `24h` | Keep processed WAL this long; `0` = no age limit |
| `WAL_RETENTION_SIZE` | `1GiB` | Keep at most this much processed WAL; `0` = no size limit |
| `CACHE_SIZE` | `64MiB` | Document cache size; `0` disables the cache |
| `DEFAULT_LIMIT` | `100` | Documents `find_documents` returns at most when a call gives no `limit` |
| `MAX_RESULT_BYTES` | `1MiB` | Cap on the JSON size of the documents one `find_documents` call returns (at least one is returned) |

```bash
docker run -d -p 127.0.0.1:8080:8080 -v cachydb-data:/data \
  -e PORT=8080 -e DB_NAME=app -e CACHE_SIZE=256MiB -e AUTH=off \
  hovhannespalyan/cachydb
```

Arguments after the image name go to `cachydb`, and flags win over
environment variables. Prefer environment variables for `TRANSPORT`,
`PORT`, `TLS_CERT` and `TLS_KEY`: the healthcheck reads them, not the
flags. Other commands work too:

```bash
docker run --rm hovhannespalyan/cachydb version --json
docker run --rm hovhannespalyan/cachydb docs quickstart
```

## Data volume and permissions

The image declares the volume `/data`, owned by UID/GID `65532`.

- **Named volumes** (`-v cachydb-data:/data`) inherit that ownership and
  work as they are.
- **Bind mounts** (`-v /srv/cachydb:/data`) keep the host directory's
  owner. Either give it to 65532 first:

  ```bash
  sudo mkdir -p /srv/cachydb && sudo chown 65532:65532 /srv/cachydb
  ```

  or run the container as the directory's owner:
  `docker run --user "$(id -u):$(id -g)" -v /srv/cachydb:/data …`.

A data directory is locked by the process using it: only one cachydb
container can use a volume at a time. A second one on the same volume
exits with *"data directory is in use by another cachydb process"*. To
share data between agents, run one HTTP container and connect them all to
it.

`docker stop` sends SIGTERM: cachydb flushes every change to its data
segments and exits 0. The default 10-second stop timeout is enough for
typical workloads.

## Upgrading

A data volume written by CachyDB 0.0.4 to 0.0.6 (on-disk format 2) is
upgraded in place the first time a newer image opens it: each collection
gets an `index.db` built from its data, and the old hint files and index
snapshots are removed. The log shows one line per collection
(`upgrading data directory format 2 -> 3; building index`). If the
container stops during the upgrade, the next start runs it again.

The upgrade is **one-way**: older images cannot open the volume
afterwards. Before you switch tags, stop the container and copy the
volume (or export it with the old image's `cachydb utils export`):

```bash
docker run --rm -v cachydb-data:/from -v cachydb-data-backup:/to alpine cp -a /from/. /to/
```

## Healthcheck

The image has a built-in `HEALTHCHECK` that runs `cachydb healthcheck`.
It probes `GET http://127.0.0.1:$PORT/healthz` (`https://` when
`TLS_CERT` and `TLS_KEY` are set, without verifying the certificate),
which answers `200 {"status":"ok","version":"…"}` while the database is
open and `503` while it shuts down; it never needs a token. With
`TRANSPORT=stdio` there is nothing to probe and the check always passes.

```bash
docker inspect -f '{{.State.Health.Status}}' cachydb   # starting, healthy, unhealthy
curl http://localhost:7601/healthz
```

## Docker Compose

```yaml
services:
  cachydb:
    image: hovhannespalyan/cachydb:latest
    ports:
      - "127.0.0.1:7601:7601"   # localhost only: there is no token (AUTH=off)
    volumes:
      - cachydb-data:/data
    environment:
      DB_NAME: main
      AUTH: "off"               # or set BOOTSTRAP_ADMIN_TOKEN and remove this line
    restart: unless-stopped

volumes:
  cachydb-data:
```

The service inherits the image's healthcheck. Other services in the same
Compose project reach it at `http://cachydb:7601/mcp`, without a token
while `AUTH` is `off`; without that line the container needs a user (see
Quick start).

## MCP client configuration

### Claude Code

```bash
# HTTP: the container runs separately (see Quick start)
claude mcp add --transport http cachydb http://localhost:7601/mcp

# stdio: Claude Code starts a container per session
claude mcp add --transport stdio cachydb -- \
  docker run -i --rm -v cachydb-data:/data -e TRANSPORT=stdio hovhannespalyan/cachydb
```

### Claude Desktop

In `claude_desktop_config.json`, then restart Claude Desktop:

```json
{
  "mcpServers": {
    "cachydb": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-v", "cachydb-data:/data",
               "-e", "TRANSPORT=stdio", "hovhannespalyan/cachydb"]
    }
  }
}
```

### Other clients (generic JSON)

stdio, in the common `mcpServers` shape:

```json
{
  "mcpServers": {
    "cachydb": {
      "command": "docker",
      "args": ["run", "-i", "--rm", "-v", "cachydb-data:/data",
               "-e", "TRANSPORT=stdio", "-e", "DB_NAME=main", "hovhannespalyan/cachydb"]
    }
  }
}
```

HTTP, for clients that support remote servers:
`{"mcpServers": {"cachydb": {"url": "http://localhost:7601/mcp"}}}`
(VS Code: `{"servers": {"cachydb": {"type": "http", "url": "http://localhost:7601/mcp"}}}`).

Each stdio session holds the volume's lock while it runs, so two clients
cannot use the same volume over stdio at once. Use the HTTP container for
several clients, or give each stdio client its own volume.

## Documentation

- The image documents itself:
  `docker run --rm hovhannespalyan/cachydb docs` lists the topics
  (overview, quickstart, mcp, tools, query, schema, config, access, errors). Over
  MCP they are the resources `cachydb://docs/<topic>`.
- README, storage design and the Go library API:
  <https://github.com/hop-/cachydb>
- Issues: <https://github.com/hop-/cachydb/issues>

## License

MIT. See [LICENSE](https://github.com/hop-/cachydb/blob/master/LICENSE).

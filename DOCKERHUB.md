# CachyDB

A lightweight document database with a built-in
[MCP (Model Context Protocol)](https://modelcontextprotocol.io) server,
written in Go. Databases hold collections of JSON documents, with optional
schemas and hash indexes. Every acknowledged write is fsynced to a
write-ahead log first, so it survives a crash. AI agents use it through 12
MCP tools (create collections, insert, find, update, delete, index).

- Source and full documentation: <https://github.com/hop-/cachydb>
- Small static image: distroless, no shell, runs as the non-root user
  `65532`
- Platforms: `linux/amd64`, `linux/arm64`

## Quick start

### HTTP (a long-running server)

```bash
docker run -d --name cachydb -p 7601:7601 -v cachydb-data:/data hovhannespalyan/cachydb
```

The MCP endpoint is `http://localhost:7601/mcp` (Streamable HTTP). Many
clients can connect at once. Data lives in the `cachydb-data` volume and
survives container restarts and upgrades.

There is no authentication or TLS: publish the port only on a trusted
network, for example `-p 127.0.0.1:7601:7601` for local use.

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
| `PORT` | `7601` | HTTP port inside the container (path `/mcp`) |
| `ROOT_DIR` | `/data` | Data directory. Keep it at `/data`, the volume |
| `DB_NAME` | `main` | Default database, created at startup if missing |
| `WAL_RETENTION_AGE` | `24h` | Keep processed WAL this long; `0` = no age limit |
| `WAL_RETENTION_SIZE` | `1GiB` | Keep at most this much processed WAL; `0` = no size limit |
| `CACHE_SIZE` | `64MiB` | Document cache size; `0` disables the cache |

```bash
docker run -d -p 8080:8080 -v cachydb-data:/data \
  -e PORT=8080 -e DB_NAME=app -e CACHE_SIZE=256MiB \
  hovhannespalyan/cachydb
```

Arguments after the image name go to `cachydb`, and flags win over
environment variables. Prefer environment variables for `TRANSPORT` and
`PORT`: the healthcheck reads them, not the flags. Other commands work
too:

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

## Healthcheck

The image has a built-in `HEALTHCHECK` that runs `cachydb healthcheck`.
It probes `GET http://127.0.0.1:$PORT/healthz`, which answers
`200 {"status":"ok","version":"…"}` while the database is open and `503`
while it shuts down. With `TRANSPORT=stdio` there is nothing to probe and
the check always passes.

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
      - "127.0.0.1:7601:7601"   # drop 127.0.0.1: to listen on all interfaces
    volumes:
      - cachydb-data:/data
    environment:
      DB_NAME: main
    restart: unless-stopped

volumes:
  cachydb-data:
```

The service inherits the image's healthcheck. Other services in the same
Compose project reach it at `http://cachydb:7601/mcp`.

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
  (overview, quickstart, mcp, tools, query, schema, config, errors). Over
  MCP they are the resources `cachydb://docs/<topic>`.
- README, storage design and the Go library API:
  <https://github.com/hop-/cachydb>
- Issues: <https://github.com/hop-/cachydb/issues>

## License

MIT. See [LICENSE](https://github.com/hop-/cachydb/blob/master/LICENSE).

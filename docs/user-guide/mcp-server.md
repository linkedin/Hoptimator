# MCP server

Hoptimator ships an [MCP](https://modelcontextprotocol.io) server so AI agents,
IDEs, and chat clients can drive it over the Model Context Protocol. The
server is a thin wrapper around the JDBC driver — it exposes a fixed set of
tools that map onto the catalog, the planner, and the deployer.

## Launch

After `make build install`, the start script lives at the repo root:

```bash
./start-mcp-server
```

Under the hood it runs:

```
hoptimator-mcp-server-app \
  com.linkedin.hoptimator.mcp.server.HoptimatorMcpServer \
  "jdbc:hoptimator://fun=mysql"
```

The last argument is the JDBC URL the server connects to. To target a
different namespace or kubeconfig, edit the script or invoke
`hoptimator-mcp-server-app` directly with your URL of choice. See
[JDBC driver](jdbc.md) for the full URL format.

The server speaks MCP over stdio. It does not open a network port.

## Configuring an MCP client

Most MCP clients accept a `mcpServers` config block. Pointing one at
Hoptimator looks like:

```json
{
  "mcpServers": {
    "hoptimator": {
      "command": "/absolute/path/to/Hoptimator/start-mcp-server",
      "type": "stdio"
    }
  }
}
```

Some clients require an explicit `JAVA_HOME`. If you see
`java: command not found` or version mismatches, set it in `env`:

```json
{
  "mcpServers": {
    "hoptimator": {
      "command": "/absolute/path/to/Hoptimator/start-mcp-server",
      "type": "stdio",
      "env": {
        "JAVA_HOME": "/path/to/jdk-17"
      }
    }
  }
}
```

## Tools exposed

The server registers a fixed set of tools. They split into three groups:
**discovery** (read-only catalog and pipeline state), **planning** (dry-run a
mutation), and **execution** (actually mutate state).

### Discovery (read-only)

| Tool                     | Arguments                                       | Returns                                                  |
| ------------------------ | ----------------------------------------------- | -------------------------------------------------------- |
| `fetch_schemas`          | `catalog?`                                      | All `(catalog, schema)` pairs visible to the connection. |
| `fetch_tables`           | `catalog?`, `schema?`                           | Tables, optionally filtered by catalog/schema.           |
| `describe_table`         | `table` (required), `catalog?`, `schema?`       | Columns, primary key, foreign keys.                      |
| `fetch_pipelines`        | *(none)*                                        | Every pipeline currently deployed in the namespace.      |
| `fetch_pipeline_status`  | `pipeline` (required)                           | Per-element ready/failed/message detail for a pipeline.  |
| `describe_pipeline`      | `pipeline` (required)                           | The view SQL behind a deployed pipeline.                 |

### Planning (read-only, but expensive)

| Tool   | Arguments         | Returns                                                                                  |
| ------ | ----------------- | ---------------------------------------------------------------------------------------- |
| `plan` | `sql` (required)  | The list of YAML specs the SQL would deploy. The MCP equivalent of the CLI's `!specify`. |

`plan` accepts `CREATE [OR REPLACE] MATERIALIZED VIEW ...` and `DROP ...`
statements. It does not mutate state; use it to preview a change before
calling `modify`.

### Execution (mutating)

| Tool     | Arguments         | Returns                                                                       |
| -------- | ----------------- | ----------------------------------------------------------------------------- |
| `query`  | `sql` (required)  | Rows from a `SELECT`. Restricted to a small set of safe schemas (see below).  |
| `modify` | `sql` (required)  | Result of a `CREATE MATERIALIZED VIEW` / `DROP`. Actually deploys.            |

`query` is intentionally limited to schemas the server considers safe to
execute against (today: `ADS`, `PROFILE`, `METADATA`, `K8S`). It will refuse
queries that touch other schemas — those should be inspected via
`describe_table` instead.

`modify` only accepts `CREATE [OR REPLACE] MATERIALIZED VIEW` and `DROP`
statements. Other DDL is rejected.

## Recommended agent workflow

The tool boundary is designed so an agent can be **safely autonomous on
discovery** but **gated on mutation**. A reasonable pattern:

1. **Browse.** `fetch_schemas` → `fetch_tables` → `describe_table` to find
   the right inputs and the right output sink.
2. **Draft.** Compose a `CREATE MATERIALIZED VIEW` statement.
3. **Plan.** Call `plan` with the draft. Show the resulting YAML to the user
   (or have the agent reason over it) before doing anything else. The plan
   contains *every* resource that would land in the cluster.
4. **Apply.** Only after the plan is reviewed, call `modify` with the same
   statement.
5. **Verify.** Poll `fetch_pipeline_status` until each element is `ready`, or
   surface the failing element's `message`.

If your client supports it, configure `modify` to require explicit
confirmation per call. Hoptimator does not enforce this — the gate has to
come from the host agent.

## Limitations to know

- The server does not implement MCP's resources or prompts surfaces; only
  tools.
- The connection is established at server start; namespace and credentials
  cannot be changed mid-session. Restart with a different JDBC URL to switch
  contexts.
- The `query` allowlist is hard-coded today. If you need to widen it for an
  agent use case, the change lives in `HoptimatorMcpServer.isQueryableSource`.

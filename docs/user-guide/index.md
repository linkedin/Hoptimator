# User guide

How to drive Hoptimator from a shell, an application, or an AI agent.

## Pick your interface

- **[SQL CLI](sql-cli.md)** — interactive sqlline shell. Best for ad-hoc
  exploration, dry-running plans, and writing scripts.
- **[JDBC driver](jdbc.md)** — `jdbc:hoptimator://` for any JVM application.
  The CLI and the MCP server both sit on top of it.
- **[MCP server](mcp-server.md)** — Model Context Protocol server for AI
  agents and IDEs. Wraps the JDBC driver in a fixed set of MCP tools.

## Reference

- **[DDL reference](ddl-reference.md)** — what `CREATE`, `DROP`, `PAUSE`,
  `RESUME`, `REFRESH`, `FIRE` accept, and what's intentionally not supported.
- **[Hints](hints.md)** — runtime overrides for templates and connectors,
  for both one-off tuning and engine-level passthrough.

## Looking for something else

- Operating Hoptimator on Kubernetes (CRDs, configuration, the operator):
  [Kubernetes guide](../kubernetes/index.md) *(coming soon)*.
- Adding a new database, connector, deployer, or validator:
  [Extending Hoptimator](../extending/index.md) *(coming soon)*.
- Background reading and case studies:
  [Learn more](../resources/learn-more.md).

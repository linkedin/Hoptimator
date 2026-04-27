# Hoptimator documentation

Hoptimator is a Kubernetes-native control plane for multi-hop data pipelines.
You write SQL; it figures out the topology, provisions the topics and jobs,
deploys them, and reconciles them.

This documentation is organized by what you're trying to do. Follow the path
that matches you.

## I'm new here

Start with **[Getting started](getting-started/index.md)**:

- [Quickstart](getting-started/quickstart.md) — five minutes from `git clone`
  to a running pipeline on Docker Desktop.
- [Concepts](getting-started/concepts.md) — the vocabulary the rest of the
  docs assume.
- [Architecture overview](getting-started/architecture.md) — how a SQL
  statement turns into running infrastructure.

## I want to use Hoptimator from my application or shell

See the **[User guide](user-guide/index.md)**:

- [SQL CLI](user-guide/sql-cli.md) — sqlline-based interactive shell with
  `!pipeline`, `!specify`, `!resolve` for inspecting plans before they deploy.
- [JDBC driver](user-guide/jdbc.md) — `jdbc:hoptimator://` for Java apps,
  with full connection-property reference.
- [MCP server](user-guide/mcp-server.md) — Model Context Protocol server
  for AI agents and IDEs.
- [DDL reference](user-guide/ddl-reference.md) — `CREATE VIEW`,
  `CREATE MATERIALIZED VIEW`, `DROP`, `PAUSE`/`RESUME`, triggers, and the
  built-in `k8s` system schema.
- [Hints](user-guide/hints.md) — runtime overrides for templates and
  connectors.

## I'm operating Hoptimator on Kubernetes

The Kubernetes guide (coming in a later docs phase) will cover:

- The CRDs Hoptimator installs (`Database`, `View`, `Pipeline`,
  `TableTemplate`, `JobTemplate`, `TableTrigger`, `Subscription`,
  `LogicalTable`, `Engine`, `SqlJob`).
- The operator and its reconcile loop.
- Configuration — `hoptimator-configmap`, system properties, RBAC.

## I want to extend Hoptimator

The extension guide (coming in a later docs phase) will cover:

- Writing a Deployer (`hoptimator-api`) for a new deployment target.
- Adding a new Database / connector.
- Writing a ConfigProvider, ValidatorProvider.
- TableTemplate / JobTemplate authoring.

## I'm contributing to Hoptimator

The contributor guide (coming in a later docs phase) will cover the build, the
test layout, integration tests, and the release process. Until then, the
[CONTRIBUTING](../CONTRIBUTING.md) file at the repo root has the basics.

## Background reading

[Learn more](resources/learn-more.md) — engineering blog posts and case
studies.

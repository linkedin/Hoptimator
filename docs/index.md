# Hoptimator documentation

Hoptimator is a SQL control plane for multi-system data pipelines. You write
SQL; it figures out the topology across Kafka, Flink, Venice, and whatever
else you plug in, generates the specs, deploys them, and reconciles them.

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

See the **[Kubernetes guide](kubernetes/index.md)**:

- [Operator](kubernetes/operator.md) — what `hoptimator-operator` does,
  how to deploy it, RBAC, namespace scoping, the controllers it runs.
- [CRD reference](kubernetes/crd-reference.md) — field-by-field for every
  CRD Hoptimator installs (`Database`, `View`, `Pipeline`, `TableTemplate`,
  `JobTemplate`, `TableTrigger`, `Subscription`, `LogicalTable`, `Engine`,
  `SqlJob`).
- [Templates and configuration](kubernetes/templates.md) — authoring
  TableTemplates and JobTemplates, placeholder syntax, where placeholder
  values come from (`hoptimator-configmap`, JDBC properties, system
  properties), `ConfigProvider` SPI.
- [Triggers](kubernetes/triggers.md) — operational guide for
  `TableTrigger`: cron vs status-driven firing, pause/resume, common
  patterns.

## I want to extend Hoptimator

See **[Extending Hoptimator](extending/index.md)**:

- [Adding a new data source](extending/data-sources.md) — JDBC adapter,
  `Database` CRD, `TableTemplate` authoring.
- [Deployers](extending/deployers.md) — implementing `Deployer` for a new
  deployment target, `DeployerProvider` registration, lifecycle.
- [Validators](extending/validators.md) — pre-deploy policy enforcement
  via `Validator` and `ValidatorProvider`.
- [Config providers](extending/config-providers.md) — custom
  `ConfigProvider` SPI.
- [Templates and configuration](kubernetes/templates.md) — authoring
  `TableTemplate` and `JobTemplate` (lives in the Kubernetes guide).

## I'm contributing to Hoptimator

The contributor guide (coming in a later docs phase) will cover the build, the
test layout, integration tests, and the release process. Until then, the
[CONTRIBUTING](../CONTRIBUTING.md) file at the repo root has the basics.

## Background reading

[Learn more](resources/learn-more.md) — engineering blog posts and case
studies.

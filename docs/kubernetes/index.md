# Kubernetes guide

How to deploy and operate Hoptimator on Kubernetes. The bundled deployers are
the path of least resistance — they generate Kubernetes resources and the
operator reconciles them.

## Pages

- **[Operator](operator.md)** — what `hoptimator-operator` does, how to deploy
  it, RBAC, namespace scoping, the controllers it runs.
- **[CRD reference](crd-reference.md)** — field-by-field for every CRD
  Hoptimator installs: `Database`, `View`, `Pipeline`, `TableTemplate`,
  `JobTemplate`, `TableTrigger`, `Subscription`, `LogicalTable`, `Engine`,
  `SqlJob`.
- **[Templates and configuration](templates.md)** — authoring
  `TableTemplate` and `JobTemplate` (matching rules, placeholder syntax,
  conditional templates, common patterns), and where placeholder values
  come from (`hoptimator-configmap`, JDBC connection properties, JVM
  system properties, the `ConfigProvider` SPI, precedence).
- **[Triggers](triggers.md)** — operational guidance for `TableTrigger`:
  cron vs status-driven firing, the pause/resume lifecycle, common
  patterns (backfills, rETL, downstream notifications).

## Related

- [Concepts](../getting-started/index.md) — vocabulary the rest of the docs
  assume; covers `Database`, `View`, `Pipeline`, `Engine`, `Connector`,
  `Deployer`, `LogicalTable`, `TableTrigger`, `Subscription`, hints.
- [Architecture](../getting-started/architecture.md) — what the operator
  is doing in the bigger picture.
- [DDL reference](../user-guide/ddl-reference.md) — SQL DDL that has YAML
  CRD equivalents.

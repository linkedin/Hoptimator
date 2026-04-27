# Architecture overview

Hoptimator turns a single SQL statement into a multi-system data pipeline. This
page traces the path from a `CREATE MATERIALIZED VIEW` to the running
infrastructure that implements it, and points out the moving parts along the
way. If you'd rather read it as a conversation, the
[engineering blog post on declarative pipelines](../resources/learn-more.md) is
a good companion.

## What Hoptimator does

Three roles, in one binary:

- **Planner.** Parses your SQL against a unified catalog and produces a logical
  plan that may span Kafka, Flink, Venice, MySQL, and anything else that has a
  registered Database. The plan handles the "multi-hop" routing implicitly:
  selecting from a CDC source automatically pulls in the topic and the engine
  that has to read it.
- **Adapter.** Translates each piece of the plan into the right form for the
  target system — table specs, connector configs, job specs.
- **Operator.** Materializes those specs as Kubernetes resources, then watches
  them and reconciles drift.

The same Hoptimator process can act as all three (the SQL CLI does), or you can
deploy the operator standalone and feed it Subscriptions from CI.

## Life of a SQL statement

```
                CREATE MATERIALIZED VIEW MY.AUDIENCE AS
                  SELECT FIRST_NAME, LAST_NAME
                  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │   JDBC driver / CLI  │   parse + validate
                       └──────────┬───────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │       Planner        │   logical → physical plan
                       │  (Calcite + rules)   │
                       └──────────┬───────────┘
                                  │
                       ┌──────────┴───────────┐
                       │                      │
                       ▼                      ▼
          ┌──────────────────────┐  ┌──────────────────────┐
          │   Source/Sink specs  │  │      Job spec        │
          │   (TableTemplate)    │  │   (JobTemplate)      │
          └──────────┬───────────┘  └──────────┬───────────┘
                     │                         │
                     └────────────┬────────────┘
                                  ▼
                       ┌──────────────────────┐
                       │       Deployer       │   apply to K8s
                       └──────────┬───────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │     Kubernetes       │
                       │ Pipeline + KafkaTopic│
                       │  + FlinkSessionJob   │
                       │     + ...            │
                       └──────────┬───────────┘
                                  │
                                  ▼
                       ┌──────────────────────┐
                       │       Operator       │   reconcile loop
                       └──────────┬───────────┘
                                  │
                                  ▼
                            data flows
```

The same flow applies whether you start from SQL (CLI, JDBC, MCP) or from a
`Subscription` CRD applied with `kubectl apply -f`.

## Step 1 — Parse and resolve

The JDBC driver (`hoptimator-jdbc`) parses the statement, then resolves every
referenced table against the **catalog**. The catalog is a union of every
`Database` registered in your Kubernetes namespace. Each Database supplies a
JDBC URL that Hoptimator dials in to enumerate schemas and tables — so a query
joining `ADS.PAGE_VIEWS` and `PROFILE.MEMBERS` can pull metadata from two
completely different systems with one logical query.

## Step 2 — Plan

The planner is built on [Apache Calcite](https://calcite.apache.org/). Its job
is to turn the logical query into a physical plan that:

- chooses connectors for each source and sink,
- inserts intermediate hops where they're required (e.g. a CDC datastream
  between an OLTP database and a streaming engine),
- folds in [TableTemplates](concepts.md#tabletemplates-and-jobtemplates),
  [JobTemplates](concepts.md#tabletemplates-and-jobtemplates), and
  [hints](concepts.md#configuration-and-hints).

The result is a `Pipeline` — a triple of *sources, sink, job* — which is what
the rest of the system operates on.

## Step 3 — Specify

Each element of the pipeline asks its **Deployer** to produce a list of
specs — usually YAML — that, if applied, would bring the element to life. This
is what `!specify` and the MCP `plan` tool show you: the literal Kubernetes
manifests Hoptimator is about to apply.

Specifications are produced from the templates registered in the catalog, with
placeholders filled in by:

- the deployer (`{{name}}`, `{{table}}`, `{{flinksql}}`, …),
- the active `ConfigProvider` (defaults pulled from `hoptimator-configmap`),
- and any [hints](concepts.md#configuration-and-hints) on the connection.

## Step 4 — Deploy

Deployers apply the specs to a target system. The defaults you get out of
the box — `K8sSourceDeployer` and `K8sJobDeployer` from the
`hoptimator-k8s` module — apply to Kubernetes, which is why every page in
this guide assumes a cluster. Both implement the `Deployer` interface from
`hoptimator-api`, so they can be replaced by anything else that knows how to
materialize a `Source`, `Sink`, or `Job`. Kubernetes is the path of least
resistance, not a hard requirement.

With the default deployers, the result is a `Pipeline` resource in the
cluster plus whatever implementation resources its templates produced — a
`KafkaTopic`, a `FlinkSessionJob`, a Venice store, etc. None of those are
"Hoptimator's" — Hoptimator just generated the spec; the relevant operator
(Strimzi, Flink Kubernetes Operator, etc.) runs the workload.

## Step 5 — Reconcile

The **operator** (`hoptimator-operator`) watches Pipelines and keeps them
healthy. It uses the same Deployer machinery as the SQL path, but inverted: any
drift between the desired pipeline (from the spec) and the actual cluster state
gets reconciled.

Triggers (`TableTrigger`, `CronJob`) plug in here too — they let upstream
events or schedules drive downstream side-effects without modeling them inside
the pipeline.

## Module map

The repo is split into focused modules. The ones you'll touch most often:

| Module                            | Role                                                                 |
| --------------------------------- | -------------------------------------------------------------------- |
| `hoptimator-api`                  | The interfaces. `Deployer`, `Engine`, `Connector`, `View`, etc.      |
| `hoptimator-jdbc`                 | Calcite-based JDBC driver. Catalog, parser, planner integration.    |
| `hoptimator-jdbc-driver`          | Lightweight wrapper that exposes the driver to standard JDBC code.   |
| `hoptimator-cli`                  | The `./hoptimator` SQL CLI (sqlline + custom commands).              |
| `hoptimator-mcp-server`           | MCP server that wraps the JDBC driver for AI agents and IDEs.        |
| `hoptimator-util`                 | Planner rules, deployment service, template engine.                  |
| `hoptimator-k8s`                  | Default Deployers, the catalog/operator glue, all CRDs.              |
| `hoptimator-operator`             | The reconciler loop and its main entry point.                        |
| `hoptimator-flink-runner`         | The runtime that executes Flink SQL jobs produced by the planner.    |
| `hoptimator-flink-adapter`        | Flink-side adapter for catalog awareness.                            |
| `hoptimator-kafka` / `-kafka-controller` | Kafka catalog and controller integration.                     |
| `hoptimator-venice`               | Venice catalog adapter.                                              |
| `hoptimator-mysql`                | MySQL catalog adapter.                                               |
| `hoptimator-logical`              | LogicalTable support — one logical entity, multiple physical tiers.  |
| `hoptimator-demodb`               | In-memory demo source used by the quickstart.                        |
| `hoptimator-avro`                 | Avro schema utilities used by the catalog/connectors.                |

A handful of modules (`hoptimator-catalog`, `hoptimator-models`,
`hoptimator-planner`) are in the tree but marked for deletion; new
contributions should not target them.

## Where to extend

- **A new data source**: usually a new Database adapter (a JDBC URL handler)
  plus a `TableTemplate` for how to deploy it. Often no Java is needed.
- **A new pipeline runtime**: write a `JobTemplate` that produces the YAML
  the runtime's operator already understands (e.g. a Beam `FlinkSessionJob`,
  a Spark Operator `SparkApplication`). Hoptimator generates the spec; the
  target operator runs the job. No `Engine` registration is required for
  this path — the `Engine` CRD is only needed if Hoptimator should submit
  *queries* (not pipelines) directly to the runtime.
- **A new deployment target**: implement `Deployer` and register it via
  `DeployerProvider`. Kubernetes is the default but not a hard requirement;
  the bundled `K8sSourceDeployer` and `K8sJobDeployer` are themselves
  examples. Anything that knows how to materialize a spec will do.
- **Different cluster configuration**: usually a `ConfigProvider` change
  rather than code.

The [Extending Hoptimator](../extending/index.md) section will cover each in
detail when it lands.

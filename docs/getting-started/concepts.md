# Concepts

Hoptimator's vocabulary is mostly familiar from SQL and Kubernetes, with a few
additions specific to multi-hop pipelines. Skim this page once and the rest of
the documentation will read more naturally.

## At a glance

| Concept             | What it is                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------ |
| **Database**        | A connection to an external system that exposes tables (Kafka, Venice, MySQL, etc.).             |
| **Catalog**         | The unified namespace that lets a single SQL statement reference tables across many databases.   |
| **View**            | A named SQL query, evaluated lazily.                                                             |
| **Materialized view** | A view backed by a running data pipeline that continuously writes results to a sink.           |
| **Pipeline**        | The set of sources, sink, and job that together implement a materialized view.                   |
| **Engine**          | An execution runtime for a job (today: Flink).                                                   |
| **Connector**       | The configuration glue that lets an engine read from or write to a database.                     |
| **Deployer**        | The component that turns a planned pipeline element into real infrastructure.                    |
| **TableTemplate**   | Declarative recipe for materializing a source/sink in a particular database.                     |
| **JobTemplate**     | Declarative recipe for materializing a job on a particular engine.                               |
| **TableTrigger**    | Fires a Kubernetes job when an upstream table changes (or on a schedule).                        |
| **LogicalTable**    | A single logical entity that spans multiple physical storage tiers (nearline, online, offline).  |
| **Subscription**    | YAML-native way to declare a materialized view; equivalent to `CREATE MATERIALIZED VIEW ... AS`. |
| **Hint**            | Key/value passed at runtime that templates and connectors can pick up.                           |

## Databases, schemas, and tables

A **Database** in Hoptimator is a Kubernetes resource that registers an external
system with the catalog. Each Database supplies a JDBC URL and a schema name:

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Database
metadata:
  name: ads-database
spec:
  schema: ADS
  url: jdbc:demodb://names=ads
  dialect: Calcite
```

Once applied, the tables behind that URL show up in the catalog under the given
schema. SQL written against the catalog can join freely across schemas — for
example a query selecting from `ADS.PAGE_VIEWS` and `PROFILE.MEMBERS` can mix
data from two completely different storage systems.

The catalog is *not exhaustive* — it shows only the Databases registered in
your Kubernetes namespace. This is by design: it bounds what a given app can see
and act on.

## Views vs. materialized views

Both are defined with SQL. The difference is what gets deployed.

| Property               | View (`CREATE VIEW`)                          | Materialized View (`CREATE MATERIALIZED VIEW`)                                  |
| ---------------------- | --------------------------------------------- | -------------------------------------------------------------------------------- |
| Stored as              | A `View` Kubernetes resource                  | A `View` resource **plus** a `Pipeline` resource                                 |
| Evaluated when         | Each time it is queried                       | Continuously, by a running job                                                   |
| Produces side effects  | No                                            | Yes — provisions topics, jobs, sinks                                             |
| Use it for             | Re-using SQL fragments, abstracting joins     | Continuously delivering data from sources to a sink                              |

A materialized view is the headline abstraction: write the SQL once, and
Hoptimator figures out the topology, the connectors, the templates, and the
job that has to run to keep the sink up to date.

## Pipelines

A **Pipeline** is the machinery behind a materialized view. It always has three
parts:

- **Sources** — the input tables (one or many), each with the connector config
  needed to read them.
- **Sink** — the output table the view writes to, again with connector config.
- **Job** — the executable that ties them together (today, a Flink SQL job).

Pipelines are first-class Kubernetes objects:

```
$ kubectl get pipelines
NAME      SQL                         STATUS
my-foo    INSERT INTO ... SELECT ...  Ready.
```

Pipelines are also visible in SQL via the built-in `k8s` schema, which lets
clients (and the MCP server) query their own deployment state.

## Engines and connectors

An **Engine** is an execution runtime — Flink today, with the design intended
to allow others. Each engine is registered in Kubernetes as an `Engine`
resource that records the JDBC URL Hoptimator uses to talk to it.

A **Connector** is the configuration that lets an engine read from or write to
a particular database — for example, a Flink Kafka connector with the right
bootstrap servers and topic name. Connectors are produced by the catalog adapter
for each database, and can be customized via [TableTemplates](#tabletemplates-and-jobtemplates)
and [hints](#hints).

## Deployers

A **Deployer** turns a planned pipeline element into real infrastructure. The
default deployer is Kubernetes-based:

- The **source/sink deployer** materializes table templates as Kubernetes
  resources (e.g. a `KafkaTopic` for a Strimzi cluster).
- The **job deployer** materializes job templates (e.g. a `FlinkSessionJob`
  for the Apache Flink Kubernetes operator).

Deployers are pluggable. Anything implementing `Deployer` from `hoptimator-api`
can take the place of the defaults. See [Extending Hoptimator](../extending/index.md)
when those docs land.

## TableTemplates and JobTemplates

Adding a Database to the catalog tells Hoptimator how to *read* from a system.
**Templates** tell Hoptimator how to *deploy* to it.

- A **TableTemplate** describes the YAML to apply when a source or sink in a
  matching database becomes part of a pipeline. It also supplies the connector
  config the engine should use.
- A **JobTemplate** describes the YAML to apply when a job is needed for the
  pipeline. The template can embed the auto-generated SQL via `{{flinksql}}`.

Templates use `{{ }}` placeholders. Some placeholders are filled in by the
deployer (`{{name}}`, `{{table}}`, `{{flinksql}}`); others come from
[ConfigProviders](#configuration-and-hints) or [hints](#hints).

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTemplate
metadata:
  name: kafka-template
spec:
  databases:
    - kafka-database
  yaml: |
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaTopic
    metadata:
      name: {{name}}
    spec:
      topicName: {{table}}
      partitions: {{kafka.partitions:1}}
      ...
  connector: |
    connector = kafka
    topic = {{table}}
    properties.bootstrap.servers = ...
```

You can usually adapt Hoptimator to a new system by writing templates rather
than code.

## TableTriggers

A **TableTrigger** runs a Kubernetes job in response to changes on a table — or
on a cron schedule. Triggers are how Hoptimator drives event-driven side
effects (refreshing offline tables, kicking off downstream batch work) without
embedding that logic in the pipeline itself.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTrigger
metadata:
  name: test-table-trigger
spec:
  schema: KAFKA
  table: existing-topic-1
  schedule: "@hourly"
  yaml: |
    apiVersion: batch/v1
    kind: Job
    ...
```

## Logical tables

A **LogicalTable** is one logical entity that physically lives in several
storage tiers — for example a feature surfaced both nearline (Kafka) and online
(Venice). Hoptimator models the tiers explicitly so a single SQL `INSERT` into
the logical table can fan out into multiple pipelines that each maintain a tier.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Database
metadata:
  name: logical
spec:
  url: jdbc:logical://nearline=kafka-database;online=venice
  schema: LOGICAL
  dialect: Calcite
```

## Subscriptions

A **Subscription** is a YAML-native way to declare a materialized view. It is
equivalent to running `CREATE MATERIALIZED VIEW ... AS ...` against the JDBC
driver — useful when you want pipelines to live in the same Git workflow as
the rest of your Kubernetes manifests.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Subscription
metadata:
  name: my-feature
spec:
  database: VENICE
  sql: |
    SELECT m.id AS KEY, m.first_name FROM PROFILE.MEMBERS m
```

## Configuration and hints

Hoptimator has two mechanisms for getting values into templates beyond the
deployer's built-in placeholders.

- **ConfigProvider** — supplies static, namespace-wide values. The default
  `K8sConfigProvider` reads them from a `hoptimator-configmap` ConfigMap. Use
  this for things like cluster endpoints that are the same for every pipeline
  in the namespace.
- **Hints** — key/value pairs supplied at connection time, either as JDBC
  properties or as fields on a Subscription. Use these for per-pipeline
  overrides like Kafka partition count or Flink parallelism.

Hints come in two flavors:

- **Template hints** override `{{ }}` placeholders directly
  (`kafka.partitions=4`, `flink.parallelism=2`).
- **Connector hints** are passed straight to the engine, scoped by connector and
  source/sink role. They are formatted
  `<connector>.<source|sink>.<config-name>` — e.g.
  `kafka.source.properties.group.id=my-group`.

Hints are advisory: if the planner picks a different physical pipeline, hints
that no longer apply are dropped.

## Engines today

Hoptimator currently ships with one execution engine, **Apache Flink**, plus
support for Kafka, Venice, MySQL, a logical-table adapter, and a `demodb`
adapter for local development. The engine and connector model is designed so
new ones can be added without changing the planner or the operator. See the
connector pages (when available) for a per-system breakdown.

## Where to next

- New to Hoptimator? Run through the [Quickstart](quickstart.md).
- Curious how it all fits together? See the [Architecture overview](architecture.md).
- Want to read the original design ideas? The
  [engineering blog posts](../resources/learn-more.md) are a good companion.

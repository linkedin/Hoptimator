# Concepts

Hoptimator's vocabulary is mostly familiar from SQL and Kubernetes, with a few
additions specific to multi-hop pipelines. Skim this page once and the rest of
the documentation will read more naturally.

> Hoptimator's parser, planner, and JDBC layer are built on
> [Apache Calcite](https://calcite.apache.org/). Calcite's
> [reference](https://calcite.apache.org/docs/reference.html) is the source
> of truth for SELECT syntax and built-in functions; this page covers what
> Hoptimator adds on top.

## At a glance

| Concept             | What it is                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------ |
| **Database**        | A connection to an external system that exposes tables (Kafka, Venice, MySQL, etc.).             |
| **Catalog**         | The unified namespace that lets a single SQL statement reference tables across many databases.   |
| **View**            | A named SQL query, evaluated lazily.                                                             |
| **Materialized view** | A view backed by a running data pipeline that continuously writes results to a sink.           |
| **Pipeline**        | The set of sources, sink, and job that together implement a materialized view.                   |
| **Engine**          | A runtime Hoptimator can submit *queries* to (e.g. a Flink SQL gateway). Optional. Pipeline materialization does *not* require one. |
| **Connector**       | Configuration that tells a runtime how to read from or write to a database. Used by the planner and embedded in template output. |
| **Deployer**        | The component that turns a planned pipeline element into real infrastructure.                    |
| **Validator**       | Pre-deploy check that rejects SQL, CRDs, or planned pipelines that violate environment policy.    |
| **TableTemplate**   | Declarative recipe for materializing a source/sink in a particular database.                     |
| **JobTemplate**     | Declarative recipe for materializing a job on a particular engine.                               |
| **TableTrigger**    | Fires a Kubernetes job when an upstream table changes (or on a schedule).                        |
| **LogicalTable**    | An abstraction model: one named entity that physically lives in many backends (nearline / online / offline). Auto-syncs and auto-backfills its tiers. |
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

## Connectors

A **Connector** is the configuration that lets a runtime read from or write
to a particular database — for example, a Flink Kafka connector with the
right bootstrap servers and topic name. Connectors are produced by the
catalog adapter for each database, embedded in the YAML that
[TableTemplates](#tabletemplates-and-jobtemplates) and
[JobTemplates](#tabletemplates-and-jobtemplates) emit, and can be customized
via [hints](#hints).

Connectors do not require an `Engine` to function. The typical flow is:
Hoptimator generates a `FlinkSessionJob` (or similar) with the connector
config baked in, and an existing operator — like the Apache Flink Kubernetes
Operator — picks it up and runs the job. Hoptimator is not in the data path.

## Engines (optional)

An **Engine** CRD registers a runtime Hoptimator can submit **queries** to —
typically a Flink SQL gateway behind a JDBC URL. This is the path used when
Hoptimator needs to *execute* SQL itself, e.g. for interactive `SELECT`
against tables that aren't in-process.

Engines are unrelated to pipeline materialization. You do not need to
register an Engine for `CREATE MATERIALIZED VIEW` to work — the resulting
pipeline runs on whatever runtime the JobTemplate produces (a Flink session
job, a Beam job, a custom operator, etc.). The Engine surface is mainly used
by interactive query paths and is partially developed today.

## Deployers

A **Deployer** turns a planned pipeline element into real infrastructure.
Deployers are the actual extension point that decides where pipelines land —
**Kubernetes is the default, not a hard requirement**. The bundled deployers
in `hoptimator-k8s` target Kubernetes:

- The **source/sink deployer** materializes table templates as Kubernetes
  resources (e.g. a `KafkaTopic` for a Strimzi cluster).
- The **job deployer** materializes job templates (e.g. a `FlinkSessionJob`
  for the Apache Flink Kubernetes operator).

Anything implementing `Deployer` from `hoptimator-api` can take their place.
See [Extending Hoptimator](../extending/index.md) when those docs land.

## Validators

A **Validator** inspects a SQL statement, a CRD, or a planned pipeline
element *before* it deploys and rejects it if it doesn't meet your
constraints. Where `Deployer` is "make this real," `Validator` is "check
this is allowed."

Validators run at three points in the DDL path: on the parsed SQL, on the
resolved view/source/sink after planning, and on the deployer collection
before any side effects. If any validator emits an error, the whole
operation aborts.

The bundled validators handle table-naming and SQL/Avro compatibility
checks. Custom validators are typically used for environment-specific
policy — naming conventions, ACL enforcement, schema-evolution rules.
See [Validators](../extending/validators.md) for authoring.

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

A **TableTrigger** runs a Kubernetes job when an upstream table changes — or
on a cron schedule. The job spec is arbitrary YAML. That combination is more
powerful than it looks at first glance, and it's where a lot of Hoptimator's
operational flexibility comes from.

A trigger lets you express things that would otherwise require either a
purpose-built pipeline or a separate orchestrator:

- **Backfills** that fire when a new offline partition arrives, populating
  the offline tier of a [LogicalTable](#logical-tables) without coupling
  that work to the streaming pipeline.
- **rETL refreshes** on cron, exporting a Kafka topic to a downstream system
  on schedule.
- **Downstream notifications** — a job that calls an API, sends a Slack
  message, or kicks off a CI workflow whenever a table changes.
- **Operational hooks** — invalidating caches, rotating credentials, running
  ad-hoc validation, all driven by the same data events that pipelines react
  to.

The status field on a `TableTrigger` is what *fires* the job — typically
patched by the producer of the upstream table (or by Hoptimator itself when
data arrives). That separation — declarative trigger spec, imperative
status patch — makes triggers cleanly composable with whatever already owns
the upstream system.

Triggers can also be auto-generated from a `TableTemplate`, so adapters can
ship sensible defaults (e.g. "every Kafka source automatically gets a
backfill trigger pointing at its offline mirror") instead of asking users
to declare them by hand.

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

The pattern: pipelines stay pure data-flow expressions, triggers carry the
imperative side effects, and the two compose at the table level.

## Logical tables

A **LogicalTable** is an abstraction model over physical stores: one named
dataset that simultaneously lives in several backends. The same audience
table might exist as a Kafka topic for streaming consumers, a Venice store
for online lookups, and an HDFS dataset for batch analytics — typically
named differently in each place and tied together by hand-rolled sync jobs.
A LogicalTable replaces that arrangement with a single declaration that
binds each backend to a named tier role.

You declare the entity once. Hoptimator handles the rest — the physical
tier resources, the inter-tier pipelines that keep them in sync, the
backfill triggers, the schema reconciliation. Your SQL refers to one name;
the abstraction handles the fan-out.

### The tier model

Hoptimator recognizes three tier roles:

| Tier         | Typical backend         | Used for                                                              |
| ------------ | ----------------------- | --------------------------------------------------------------------- |
| **nearline** | Kafka, Pulsar, Brooklin | Streaming reads/writes; the "source of truth" for schema.             |
| **online**   | Venice, Redis, Pinot    | Low-latency point lookups for serving (sub-millisecond, key-based).   |
| **offline**  | HDFS, Iceberg, S3       | Batch analytics, training data, historical queries.                   |

A LogicalTable can bind any subset of these to physical Databases:

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Database
metadata:
  name: logical
spec:
  url: jdbc:logical://nearline=kafka-database;online=venice;offline=hdfs-database
  schema: LOGICAL
  dialect: Calcite
```

Every table that shows up under the `LOGICAL` schema with this configuration
inherits the three-tier topology automatically.

### What you get for free

Declaring a LogicalTable with two or more tiers is *not* just a schema
alias. The `LogicalTableDeployer` runs at deploy time and produces real
infrastructure for each binding:

- **Physical tier resources.** Each tier's backing Database goes through
  the normal Deployer SPI to create whatever the storage system needs (a
  Kafka topic, a Venice store, an HDFS dataset).
- **Implicit inter-tier pipelines.** Hoptimator auto-deploys
  `nearline → online` and `nearline → offline` Pipeline CRDs to keep the
  tiers consistent. You don't write the Kafka-to-Venice job; it appears
  because the LogicalTable says it should.
- **Auto-backfill triggers.** When an offline tier is present, a
  `TableTrigger` is created so the offline mirror can be backfilled and
  refreshed without an external orchestrator. (See
  [TableTriggers](#tabletriggers) for what this enables.)
- **One schema, one source of truth.** The row type is resolved from the
  nearline tier (or the first available one) and reused everywhere. You
  declare columns once, in the place where they're naturally streamed.

### Why this matters as an abstraction

Multi-tier datasets are usually built imperatively: pick a name for the
Kafka topic, pick a different name for the Venice store, pick a third for
the HDFS dataset, write a sync job, write a backfill job, hope nobody ever
forgets to update one of them when the schema changes. LogicalTables turn
that into a declarative model with three concrete benefits:

- **Application code is tier-agnostic by default.** Reading from
  `LOGICAL.audience` Just Works; the application doesn't have to know that
  the *online* read goes to Venice. Switching tier backends is a CRD edit,
  not a code change.
- **The "right" topology is the cheap path.** Standing up a multi-tier
  feature is one declaration instead of N hand-rolled pieces. The
  hard-to-skip parts (sync jobs, backfill triggers) come along
  automatically, so they don't get skipped.
- **Materialized views compose with it cleanly.**
  `CREATE MATERIALIZED VIEW LOGICAL."audience$members" AS …` writes to the
  logical table and the inter-tier syncing fans the data out across all
  bound tiers. Many writers can use the
  [partial-view](../user-guide/ddl-reference.md#partial-views-multiple-pipelines-into-one-sink)
  pattern to share the same logical sink.

### The classic use case: feature stores and lambda architecture

The pattern this most directly enables is the
[Lambda / Kappa architecture](https://en.wikipedia.org/wiki/Lambda_architecture)
that feature stores are built on:

- A *streaming* path keeps the freshest values flowing through nearline.
- A *serving* path exposes the same data to online queries with low latency.
- A *batch* path lets analytics and ML training jobs read historical
  snapshots from offline.

Today this typically means three different systems with three different
names and three different sync mechanisms. With a LogicalTable, it's one
name, and the sync mechanisms are themselves declared infrastructure.

### Implementation note

Mechanically, LogicalTables are exposed via a JDBC driver
(`jdbc:logical://…`) so they show up in the catalog like any other
Database, but conceptually the logical table is an *abstraction model* over
the physical stores — most of the value is realized by the deployer when
you create one, not by the driver at query time. Read it as "one logical
entity backed by N physical stores," not "another connector."

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

## Bundled adapters and runtimes

Hoptimator ships with adapters for **Kafka**, **Venice**, **MySQL**, the
**logical-table** tier model, and a `demodb` source for local development.
Pipeline jobs target **Apache Flink** by default through the bundled
JobTemplates. The planner, the catalog, and the deployer model are all
designed so new sources, sinks, and runtimes can be added without changing
core code. See the connector pages (when available) for a per-system
breakdown.

## Where to next

- New to Hoptimator? Run through the [Quickstart](quickstart.md).
- Curious how it all fits together? See the [Architecture overview](architecture.md).
- Want to read the original design ideas? The
  [engineering blog posts](../resources/learn-more.md) are a good companion.

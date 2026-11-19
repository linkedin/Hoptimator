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
                CREATE MATERIALIZED VIEW ADS.AUDIENCE AS
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
`Subscription` custom resource applied with `kubectl apply -f`.

## Step 1 — Parse and resolve

The JDBC driver (`hoptimator-jdbc`) parses the statement, then resolves every
referenced table against the **catalog**. The catalog is a union of every
`Database` registered in your Kubernetes namespace. Each Database supplies a
JDBC URL that Hoptimator dials in to enumerate schemas and tables — so a query
joining `ADS.PAGE_VIEWS` and `PROFILE.MEMBERS` can pull metadata from two
completely different systems with one logical query.

Drivers based on `CalciteDriver` can transfer owned resources with
`CalciteDriver.addConnectionResource`. Resources close once, in reverse
registration order, when the physical connection closes, including failed
connection initialization. A pooled connection's logical return does not
dispose these resources. Register only owned resources, not shared clients.
Both public `CalciteDriver.connect` overloads are final and own acquisition,
initialization, and cleanup. Subclasses override `configureConnection` with the
acquired connection, URL, and merged properties instead of overriding `connect`
or passing an initialization callback. Calcite's `onConnectionInit` runs first;
its existing overrides still control lattice scanning and schema model loading.
`connectionProperties` merges URL properties over caller properties and can
validate before allocation. `connectionException` preserves each driver's
exception classification after cleanup. If initialization fails, the base driver
closes the connection and attaches cleanup failures to the original failure as
suppressed exceptions. Successful initialization transfers ownership to the caller.
External subclasses overriding `connect` must migrate to these hooks and be
rebuilt against the updated driver. `HoptimatorDriver` remains a separate wrapping
facade with its own explicit failure-cleanup boundary.
`VeniceDriver` registers closeable cluster schemas before publishing them and
disposes the connection if schema creation or registration fails. This cleanup
does not define when a shared connection pool is evicted or shut down.

For Kubernetes `Database` catalogs, `K8sDatabaseTable` uses
`JdbcDataSourcePool.shared()` rather than Calcite's soft-value pool cache.
The owner preserves soft retention and the existing effective URL, driver,
username, and password key. Each outer Hoptimator connection holds
a catalog-lifetime lease that strongly pins the pool's ownership token, so lazy
metadata reads can still borrow connections after a pool is retired.
`invalidate`/`invalidateAll` remove pools from reuse;
physical disposal waits for the last catalog lease to close. Normal outer
connection close releases its leases without discarding still-cached pools.
Failed outer initialization also releases registered leases.

Once no catalog lease pins a token, it is only softly reachable. GC can reclaim
it, and one shared Java `Cleaner` closes the associated pool, which physically
closes its idle connections and their explicitly owned resources. Cleanup state
retains the pool but not the token; otherwise GC-driven cleanup would never run.
Closed leases release their tokens even if the lease handles themselves remain
reachable. Asynchronous cleanup failures are logged; explicit retirement and
lease-close failures are returned to the caller.

Calcite's pool cache has no supported removal-listener hook, so this cleanup is
limited to Hoptimator's Kubernetes Database ownership helper. No Calcite internals
are modified or accessed reflectively. The shared owner also registers one JVM
shutdown hook and can be closed explicitly; close rejects new catalog acquisitions
and drains existing leases. There is no automatic idle timeout, capacity limit,
or freshness interval: soft reclamation depends on GC and is not time-bounded.
Schema/table metadata caching is unchanged. Engine data sources and direct
Calcite pool users are not migrated by the Kubernetes Database integration.

Opening a new outer connection is not a metadata-refresh boundary. It rebuilds
the Kubernetes catalog and its JDBC schema wrappers, but can reuse a downstream
physical connection, including that driver's metadata caches. In particular,
Venice's `ClusterSchema` keeps targeted table lookups and a once-loaded bulk
store listing for its physical connection's lifetime. After bulk enumeration,
later requests can miss newly created stores; a cached dropped store can still
resolve and fail when its schema is fetched. Pool retirement replaces these
caches for new requests without breaking catalogs still leasing the old pool.
This is a freshness limitation, distinct from resource ownership, not a reason
to close shared pools whenever an individual request finishes. Lifecycle
integration tests verify create/drop visibility after explicit pool retirement,
not an unconditional refresh whenever an outer connection opens.

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

Deployers apply the specs. The bundled `K8sSourceDeployer` and
`K8sJobDeployer` (in `hoptimator-k8s`) target Kubernetes — which is why
every page in this guide assumes a cluster — but the `Deployer` SPI is
the swap point.

With the default deployers, the result is a `Pipeline` resource plus
whatever implementation resources its templates produced — a `KafkaTopic`,
a `FlinkSessionJob`, a Venice store. None of those are "Hoptimator's";
Hoptimator generated the spec, and the relevant operator (Strimzi, Flink
Kubernetes Operator, etc.) runs the workload.

## Step 5 — Reconcile

The **operator** (`hoptimator-operator`) watches Pipelines and keeps them
healthy. It uses the same Deployer machinery as the SQL path, but inverted: any
drift between the desired pipeline (from the spec) and the actual cluster state
gets reconciled.

Triggers (`TableTrigger`, `CronJob`) plug in here too — they let upstream
events or schedules drive downstream side-effects without modeling them inside
the pipeline.

## Creating a table without SQL

A single table (a `Source`) doesn't need a query to describe it — just a row
type. For that case there's a SQL-free, **connection-free** entrypoint,
`TableService.create(...)` (and `TableService.delete(...)`) in
`hoptimator-jdbc`, which takes connection-level properties plus a table path and
an **Avro schema** and runs the *same* validation and deployment as
`CREATE TABLE` — the row type is derived from the Avro schema (via
`AvroConverter`) instead of from SQL column declarations. Both paths converge on
`HoptimatorDdlUtils.deployTableInternal`, so validators, deployers, and rollback
behave identically regardless of where the schema came from.

This is what lets a caller (e.g. a gRPC service) create a table by handing over
a name and a schema, with no SQL parsing, no planning, and no JDBC connection. A
`dryRun` flag mirrors `!specify` / the MCP `plan` tool: it validates and renders
the specs without deploying anything.

This works because the deploy/validate SPI is decoupled from Calcite. Providers
(`Deployer`, `Validator`, `Connector`, `Config`) receive a neutral
`DeploymentContext` — not a `java.sql.Connection` — that exposes only what they
need: connection-level properties and per-`Database` config. The row type is
resolved on demand via `HoptimatorDriver.rowType(source, context)`. The SQL path
supplies a Calcite-backed `DeploymentContext` (row type read from the catalog);
the direct path supplies a `DirectDeploymentContext` that carries the caller's
Avro schema (deriving the row type on demand) and resolves `Database` config
registry-natively via a `DatabaseConfigResolver`
(the K8s implementation reads `Database` custom resources directly — see
`DatabaseConfigResolvers`). The direct path opens no connection and touches no
Calcite catalog; only the SQL engine's read/plan path still uses the JDBC
driver layer.

## Module map

The repo is split into focused modules. The ones you'll touch most often:

| Module                                   | Role                                                                |
|------------------------------------------|---------------------------------------------------------------------|
| `hoptimator-api`                         | The interfaces. `Deployer`, `Engine`, `Connector`, `View`, etc.     |
| `hoptimator-jdbc`                        | Calcite-based JDBC driver. Catalog, parser, planner integration.    |
| `hoptimator-jdbc-driver`                 | Lightweight wrapper that exposes the driver to standard JDBC code.  |
| `hoptimator-cli`                         | The `./hoptimator` SQL CLI (sqlline + custom commands).             |
| `hoptimator-mcp-server`                  | MCP server that wraps the JDBC driver for AI agents and IDEs.       |
| `hoptimator-util`                        | Planner rules, deployment service, template engine.                 |
| `hoptimator-k8s`                         | Default Deployers, the catalog/operator glue, all CRDs.             |
| `hoptimator-operator`                    | The reconciler loop and its main entry point.                       |
| `hoptimator-flink-runner`                | The runtime that executes Flink SQL jobs produced by the planner.   |
| `hoptimator-flink-adapter`               | Flink-side adapter for catalog awareness.                           |
| `hoptimator-kafka` / `-kafka-controller` | Kafka catalog and controller integration.                           |
| `hoptimator-mysql`                       | MySQL catalog adapter.                                              |
| `hoptimator-pinot`                       | Pinot catalog adapter.                                              |
| `hoptimator-venice`                      | Venice catalog adapter.                                             |
| `hoptimator-logical`                     | LogicalTable support — one logical entity, multiple physical tiers. |
| `hoptimator-graph`                       | Pipeline graph renderers. Ships the Mermaid backend for `!graph`.   |
| `hoptimator-demodb`                      | In-memory demo source used by the quickstart.                       |
| `hoptimator-avro`                        | Avro schema utilities used by the catalog/connectors.               |

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
  this path — the `Engine` custom resource is only needed if Hoptimator should submit
  *queries* (not pipelines) directly to the runtime.
- **A new deployment target**: implement `Deployer` and register it via
  `DeployerProvider`. Kubernetes is the default but not a hard requirement;
  the bundled `K8sSourceDeployer` and `K8sJobDeployer` are themselves
  examples. Anything that knows how to materialize a spec will do.
- **A new visualization format or graph backend**: implement `GraphRenderer`
  for a new output format (DOT, JSON, an interactive HTML view, …) or
  `GraphProvider` to build the dependency graph from somewhere other than
  Kubernetes. The bundled Mermaid renderer (`hoptimator-graph`) and K8s
  provider (`hoptimator-k8s`) are reference impls.
- **Different cluster configuration**: usually a `ConfigProvider` change
  rather than code.

The [Extending Hoptimator](../extending/index.md) section will cover each in
detail when it lands.

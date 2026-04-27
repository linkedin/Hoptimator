# Adding a new data source

Adding a new external system to Hoptimator usually means three things:

1. A **JDBC adapter** that exposes the system's tables and schemas through a
   JDBC connection — Hoptimator reads metadata through this.
2. A **`Database` CRD** that registers the adapter (a JDBC URL plus a
   schema name) so the catalog includes it.
3. A **`TableTemplate`** (and possibly a **`JobTemplate`**) that tells
   Hoptimator how to deploy resources for the system — Kafka topics, Venice
   stores, etc. — and what connector configs the engine should use to
   read/write them.

Often you only need step 2 plus templates: if there's already a JDBC driver
that talks to your system, you can register it without writing any Java.
The walkthrough below covers the full path; skip the parts you don't need.

## The JDBC adapter

A Hoptimator adapter is a regular `java.sql.Driver` that returns a Calcite
connection over your URL scheme (`jdbc:my-system://...`). The driver's job
is to expose the system's tables to Calcite — once Calcite can see them,
the planner can route queries through them.

Bundled adapters:

- [`hoptimator-demodb`](https://github.com/linkedin/Hoptimator/tree/main/hoptimator-demodb)
  — the smallest end-to-end example. `DemoDriver` extends `CalciteDriver`,
  parses the URL, and registers in-memory schemas (`AdsSchema`,
  `ProfileSchema`) with hard-coded tables. Read this first.
- [`hoptimator-kafka`](https://github.com/linkedin/Hoptimator/tree/main/hoptimator-kafka),
  [`hoptimator-venice`](https://github.com/linkedin/Hoptimator/tree/main/hoptimator-venice),
  [`hoptimator-mysql`](https://github.com/linkedin/Hoptimator/tree/main/hoptimator-mysql)
  — progressively richer versions of the same pattern, each backing onto
  a real cluster API for table discovery.

The shape every adapter has in common:

- Extends `CalciteDriver` (from `hoptimator-jdbc`).
- Declares a URL prefix via `getConnectStringPrefix()`.
- In `connect()`, parses the URL parameters and calls `super.connect()`
  to get a `CalciteConnection`, then registers one or more `Schema`
  instances on its root.
- Schemas implement Calcite's `Schema` (typically extending
  `AbstractSchema`); their `getTables()` returns the tables you want
  exposed, each with a row type and any metadata the templates will need.

Register the driver via `META-INF/services/java.sql.Driver`:

```
com.example.hoptimator.mysystem.MySystemDriver
```

## Registering with the catalog

Once your driver is on the classpath, a `Database` CRD makes it visible to
Hoptimator:

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Database
metadata:
  name: my-system
spec:
  schema: MYSYS
  url: jdbc:my-system://endpoint=my-system.internal:1234
  dialect: Calcite
```

`schema` is the catalog name your tables show up under. `url` is whatever
your driver expects. See the
[Database CRD reference](../kubernetes/crd-reference.md#database) for all
fields.

After applying the CRD, Hoptimator's catalog picks it up on the next
connection — `!tables` in the SQL CLI should show your system's tables.

## Telling Hoptimator how to deploy resources

Reading from a system is one job; *deploying* to it is another. Two paths:

### Path A: declarative, via templates

If your system has an existing operator (Strimzi for Kafka, the Flink
Kubernetes Operator, etc.) that already takes YAML and provisions
resources, you can ship a `TableTemplate` that emits that YAML when
Hoptimator wants to materialize a table:

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTemplate
metadata:
  name: my-system-template
spec:
  databases: [ my-system ]
  yaml: |
    apiVersion: my-system.example.com/v1
    kind: MyResource
    metadata:
      name: {{name}}
    spec:
      key: {{table}}
  connector: |
    connector = my-system
    endpoint = {{my-system.endpoint}}
    target = {{table}}
```

No Java. The bundled Kafka deployment uses this pattern — Hoptimator emits
a `KafkaTopic` CRD; Strimzi creates the topic. See
[Templates](../kubernetes/templates.md) for the placeholder syntax and
matching rules.

### Path B: imperative, via a custom Deployer

If your system needs an admin API call rather than a YAML CRD apply (e.g.
calling a Venice controller's REST endpoint), you'll need a custom
`Deployer`. This is the path the bundled `hoptimator-venice` and
`hoptimator-kafka` modules take in addition to (or instead of) templates.
See [Deployers](deployers.md) for the full guide.

You can ship both — Path A for systems where YAML is enough, Path B for the
ones that aren't.

## When you only need a connector, not a deployer

Sometimes the upstream resource already exists and you only want
Hoptimator to be able to read/write it via an engine. In that case, ship a
`TableTemplate` with **only** the `connector` field — no `yaml`. The
template tells the engine how to address the existing resource without
provisioning anything new:

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTemplate
metadata:
  name: external-kafka-connector
spec:
  databases: [ external-kafka ]
  connector: |
    connector = kafka
    topic = {{table}}
    properties.bootstrap.servers = external-kafka:9092
    value.format = json
```

This is the most common case for production deployments where infra is
managed elsewhere and Hoptimator just orchestrates the data flow.

## Verifying

```sql
-- Adapter is loaded and the catalog sees your tables
0: Hoptimator> !schemas
0: Hoptimator> !tables MYSYS

-- Templates render correctly without deploying
0: Hoptimator> !specify CREATE MATERIALIZED VIEW MYSYS.foo AS SELECT * FROM ...

-- End-to-end: actually deploy
0: Hoptimator> CREATE MATERIALIZED VIEW MYSYS.foo AS SELECT * FROM ...
0: Hoptimator> !quit
$ kubectl get pipelines
```

## When to write a connector provider

`ConnectorProvider` is a separate SPI from the JDBC driver and the
`Deployer`. It's used when the planner needs to decide *which* connector
config to use for a particular source/sink in a pipeline — typically when
the choice depends on runtime context (e.g. a different connector for the
same database under different security postures).

For most adapters this isn't needed; the `connector` field on the
`TableTemplate` is sufficient. Reach for `ConnectorProvider` only when the
choice can't be expressed declaratively. The bundled
`K8sConnectorProvider` is the reference implementation.

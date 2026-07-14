# Extending Hoptimator

Hoptimator's behavior is driven by Java SPI plug-ins (`ServiceLoader`-based)
and by the `TableTemplate` / `JobTemplate` CRDs. Most extensions don't need
both — pick the layer that matches what you're doing.

## Pick the right surface

| You want to…                                                                                        | What you'll write                                                  |
|-----------------------------------------------------------------------------------------------------| ------------------------------------------------------------------ |
| Connect a new external system to the catalog (Kafka, Venice, MySQL, your-system).                   | A JDBC adapter + `TableTemplate` / `JobTemplate`. See [Data sources](data-sources.md). |
| Send Hoptimator-generated specs somewhere other than Kubernetes.                                    | A `Deployer` + `DeployerProvider`. See [Deployers](deployers.md).   |
| Reject SQL or YAML that's invalid in your environment before it deploys.                            | A `Validator` + `ValidatorProvider`. See [Validators](validators.md). |
| Pull configuration values from somewhere other than `hoptimator-configmap`.                         | A `ConfigProvider`. See [Config providers](config-providers.md).    |
| Build a dependency graph from some backing store (e.g. K8s).                                        | A `GraphProvider`. The K8s-backed default ships in `hoptimator-k8s`. |
| Render the dependency graph in a format other than the ones shipped (DOT, an interactive web view, …). | A `GraphRenderer`. Mermaid and JSON renderers ship in `hoptimator-graph`. |
| Customize what gets deployed for an existing system.                                                | Just a `TableTemplate` or `JobTemplate` — no Java needed. See [Templates and configuration](../kubernetes/templates.md). |
| Fire a `TableTrigger` when your source has new data (event-time).                                   | An `InputFrontierSource` on your driver's schema. See [Firing triggers on data availability](#firing-triggers-on-data-availability). |

## How extensions are loaded

All extension points are loaded via Java's `ServiceLoader`. To register
an implementation, drop a service file under
`src/main/resources/META-INF/services/` named after the SPI interface:

```
META-INF/services/com.linkedin.hoptimator.DeployerProvider
META-INF/services/com.linkedin.hoptimator.ValidatorProvider
META-INF/services/com.linkedin.hoptimator.ConfigProvider
META-INF/services/com.linkedin.hoptimator.ConnectorProvider
META-INF/services/com.linkedin.hoptimator.CatalogProvider
META-INF/services/com.linkedin.hoptimator.graph.GraphProvider
META-INF/services/com.linkedin.hoptimator.graph.GraphRenderer
```

Each file contains the fully qualified class name(s) of your
implementation, one per line. Empty lines and lines starting with `#` are
ignored — you can use `#` to leave registrations in the file but disabled
(see `hoptimator-kafka` for an example of an SPI that's commented out by
design).

When Hoptimator starts, every implementation on the classpath is loaded.
For surfaces that produce multiple values for the same input — `Validator`,
`ConfigProvider`, `Connector` — all matching providers contribute. For
`Deployer`, `DeployerProvider`s have a `priority()` and are tried in order.

## Common patterns

### "I just want to add my system to the catalog"

The lowest-friction path is **a JDBC driver + a `Database` CRD**. Hoptimator
treats anything that responds to a JDBC URL as a potential catalog source.
You point a `Database` at it, and Hoptimator pulls schemas and tables from
that connection. See [Data sources → Adapter](data-sources.md#the-jdbc-adapter).

### "I need Hoptimator to actually deploy my system's resources"

After the adapter, ship a `TableTemplate` (or `JobTemplate`) that emits the
YAML for your storage system's CRD or operator. Templates are a CRD, so
this is YAML-only — no Java needed. See
[Templates and configuration](../kubernetes/templates.md).

If your storage system needs **imperative provisioning** (calling an admin
API to create a topic, store, table), you need a `Deployer` instead of —
or in addition to — a template. See [Deployers](deployers.md).

### "I want to enforce policy"

Use a `Validator`. Unlike a `Deployer`, validators run **before** any
mutation, and the SQL/YAML is rejected if a validator returns errors.
Common uses: naming conventions, schema compatibility, ACL checks. See
[Validators](validators.md).

### "I want to visualize what's deployed differently"

The `!graph` CLI command (see
[SQL CLI → !graph](../user-guide/sql-cli.md#graph-identifier---depth-n))
goes through two SPIs: `GraphProvider` builds the typed
`PipelineGraph` from some backing store, and `GraphRenderer` serializes
it to a string. The bundled defaults are a K8s-backed
`K8sGraphProvider` (in `hoptimator-k8s`) plus `MermaidRenderer` and
`JsonGraphRenderer` (in `hoptimator-graph`).

Add a `GraphRenderer` to support a new output format (e.g. DOT for
graphviz, an interactive web view). Add a `GraphProvider` if the
pipeline state lives somewhere other than Kubernetes — the K8s
implementation is the reference. Both register via `META-INF/services`
like every other SPI here.

### "I want a `TableTrigger` to fire when my source has new data"

#### Firing triggers on data availability

This one is **not** a `ServiceLoader` SPI. A `TableTrigger` fires when its input advances to a new
data-time *frontier*; the source reports that frontier by having the Calcite `Schema` its JDBC
driver already builds implement `com.linkedin.hoptimator.InputFrontierSource`:

```java
public interface InputFrontierSource {
  Optional<Instant> frontier(String table);                        // latest data-time seen
  default List<DataChange> changesSince(String table, Instant since) { … } // late/out-of-order repair
}
```

The `frontier` is an **optimistic** signal — "data has appeared through here," not a guarantee that
everything at or before it has arrived. Late or out-of-order writes that land behind the cursor are
reported by `changesSince` and healed with one-off backfills, so completeness is achieved by
*frontier + repair* rather than by holding the frontier back.

Because the capability hangs off the schema — the object the driver constructs from *this*
`Database`'s connection config — per-cluster configuration (which brokers to read, etc.) is
inherent: there is no global config to reach for, and many clusters can each be their own
`Database`. The source-agnostic `TableTriggerReconciler` resolves a trigger's `(catalog, schema)` to
the `Database`'s schema via `HoptimatorJdbcSchema.inputFrontierSource()` (which walks the driver's
inner schema and `unwrap`s this interface — exactly like the `LogicalSchemaMarker` one-bit marker,
just with methods), then asks it about the specific `table`. A schema that doesn't implement the
interface is simply not frontier-driven, and the trigger falls back to cron/manual `FIRE`.

To participate, have your driver's inner schema `implements InputFrontierSource`; the Kafka
`ClusterSchema` in `hoptimator-kafka` is the reference. No `META-INF/services` file is required.

## Register, then test

After dropping a service file and a class, the standard verification path
is:

1. `make build` — Gradle picks up the new SPI registration as part of the
   resource jar.
2. From the SQL CLI, run `!specify <your sql>` (or apply a Subscription)
   and look for your implementation in the rendered output / logs.
3. The operator logs the resolved deployer set when reconciling — grep
   for the class name to confirm yours was selected.

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

## Register, then test

After dropping a service file and a class, the standard verification path
is:

1. `make build` — Gradle picks up the new SPI registration as part of the
   resource jar.
2. From the SQL CLI, run `!specify <your sql>` (or apply a Subscription)
   and look for your implementation in the rendered output / logs.
3. The operator logs the resolved deployer set when reconciling — grep
   for the class name to confirm yours was selected.

# Templates and configuration

`TableTemplate` and `JobTemplate` are how you tell Hoptimator what to deploy
when a table or job becomes part of a pipeline. Templates are the
customization workhorse — most adapters can be added without writing Java
just by authoring templates — and they're rendered against a configuration
set assembled from a Kubernetes ConfigMap, JDBC connection properties, and
JVM system properties.

This page covers both: how to author templates, and where the values they
substitute come from.

---

## Templates

### How matching works

When the planner needs to deploy a source, sink, or job, the deployer scans
all registered templates and picks the matching ones.

A `TableTemplate` matches when:

1. The source/sink's database name is in the template's `databases` list
   (or the list is empty/null, meaning "match every database").
2. The access method is in the template's `methods` list (or the list is
   empty/null). Access methods are:
   - `Scan` — the table is being read from.
   - `Modify` — the table is being written to.

A `JobTemplate` matches when the pipeline's database is in the template's
`databases` list (or the list is empty/null).

Multiple matching templates all contribute their YAML, so a single source
can produce several Kubernetes resources by stacking templates. This is
how, for example, the demo deploys both a Strimzi `KafkaTopic` *and* a
`TableTrigger` for the same Kafka source — see
`deploy/samples/demodb.yaml`.

### TableTemplate structure

A `TableTemplate` may set either or both of `yaml` and `connector`:

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTemplate
metadata:
  name: kafka-template
spec:
  databases: [ kafka-database ]
  methods: [ Scan, Modify ]
  yaml: |
    apiVersion: kafka.strimzi.io/v1
    kind: KafkaTopic
    metadata:
      name: {{name}}
    spec:
      topicName: {{table}}
      partitions: {{kafka.partitions:1}}
      replicas: 1
  connector: |
    connector = kafka
    topic = {{table}}
    properties.bootstrap.servers = {{kafka.bootstrap.servers}}
    value.format = json
```

- `yaml` — a Kubernetes manifest (or several, separated by `---`) that
  Hoptimator applies when this source/sink is deployed.
- `connector` — a properties-style block consumed by the engine. For Flink,
  it shows up in the auto-generated `CREATE TABLE … WITH (…)` clause.

A template that supplies only `connector` is useful when the upstream
infrastructure already exists — the connector tells the engine how to
read/write it without deploying anything new.

### JobTemplate structure

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: JobTemplate
metadata:
  name: flink-template
spec:
  yaml: |
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkSessionJob
    metadata:
      name: {{name}}
    spec:
      deploymentName: basic-session-deployment
      job:
        entryClass: com.linkedin.hoptimator.flink.runner.FlinkRunner
        args:
        - {{flinksql}}
        jarURI: file:///opt/{{flink.app.name}}.jar
        parallelism: {{flink.parallelism:1}}
        upgradeMode: stateless
        state: running
```

The `flinksql` placeholder gets the auto-generated pipeline SQL substituted
in, including the source/sink `CREATE TABLE` statements with their
connector configs from matching `TableTemplate`s.

### Placeholder syntax

Placeholders are `{{...}}` substitutions resolved at render time:

| Form                                  | Meaning                                                                |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `{{var}}`                             | Substitute the value of `var`. Skips the template if missing.          |
| `{{var:default}}`                     | Substitute `var`, falling back to `default` if missing.                |
| `{{var==value}}`                      | **Template-level guard**: render this template only if `var` equals `value`; otherwise skip the whole template. The marker itself is erased from the output. |
| `{{var!=value}}`                      | **Template-level guard**: render this template only if `var` does *not* equal `value`; otherwise skip the whole template. |
| `{{var toUpperCase}}`                 | Render in upper case.                                                  |
| `{{var toLowerCase}}`                 | Render in lower case.                                                  |
| `{{var concat}}`                      | Concatenate a multi-line value into one line.                          |
| `{{var concat toUpperCase}}`          | Chain transforms.                                                      |
| `{{var:default toUpperCase}}`         | Default + transform, also chainable.                                   |

#### Multi-line values in lists and comments

If `var` is multi-line and the placeholder appears in a YAML list or
comment context, each line is repeated with the same `-` or `#` prefix:

```yaml
- {{multiline}}      # Renders as one list entry per line of `multiline`.
```

To produce a single multi-line value as one block instead, use YAML's
multi-line marker:

```yaml
- |
    {{multiline}}    # One multi-line block, properly indented.
```

### Conditional templates

The `==` / `!=` markers are **template-level guards**, not inline
conditionals. They control whether the template is used at all. When the
condition fails, the renderer returns nothing for the template and the
deployer moves on to the next matching template; when it succeeds, the
marker itself is erased from the output and the rest of the template
renders normally.

This is the mechanism for "use *this* template when X, *that* template
when Y" — you ship two templates with mirroring guards, and only one fires
for a given pipeline:

```yaml
# Template A: applies when the Flink app is a SQL job.
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: JobTemplate
metadata:
  name: flink-sql-template
spec:
  yaml: |
    {{flink.app.type==SQL}}
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkSessionJob
    metadata:
      name: {{name}}
    spec:
      job:
        entryClass: com.linkedin.hoptimator.flink.runner.FlinkRunner
        args: [ {{flinksql}} ]
        jarURI: file:///opt/hoptimator-flink-runner.jar
---
# Template B: applies when the Flink app is a Beam job.
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: JobTemplate
metadata:
  name: flink-beam-template
spec:
  yaml: |
    {{flink.app.type==Beam}}
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkSessionJob
    metadata:
      name: {{name}}
    spec:
      job:
        entryClass: com.linkedin.hoptimator.beam.runner.BeamRunner
        args: [ {{flinksql}} ]
        jarURI: file:///opt/hoptimator-flink-beam-runner.jar
```

Whichever template's guard matches the current value of `flink.app.type`
is the one that produces YAML; the other returns nothing.

The guard can sit anywhere in the template — the renderer just needs to
encounter it once. By convention, putting it on the first line makes
intent obvious.

### Authoring patterns

#### One adapter, two templates

Most adapters need at least two templates: one for `Scan` and one for
`Modify`. The `Scan` template typically configures a *source* connector
(kafka, jdbc, …); the `Modify` template configures a *sink* connector.
Splitting them lets the planner pick the right one without you having to
encode the choice in template logic. The `demodb.yaml` sample shows the
pattern:

```yaml
spec:
  methods: [ Scan ]
  connector: |
    connector = datagen
    number-of-rows = 10
---
spec:
  methods: [ Modify ]
  connector: |
    connector = blackhole
```

#### Stacking templates

Multiple matching templates all contribute. To attach an auto-generated
`TableTrigger` to every Kafka source, ship a `TableTemplate` that emits a
`TableTrigger` resource alongside the Strimzi `KafkaTopic` from the main
template. See `ads-source-trigger-template` in
`deploy/samples/demodb.yaml`.

#### Adapting an existing system without deploying anything new

If the system already exists and you only need to teach Hoptimator how to
read/write it, supply just `connector`:

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

No `yaml` field — the topic isn't created by Hoptimator; the connector just
tells Flink how to address it.

### Tips

- **Test before you deploy.** Run `!specify <sql>` from the SQL CLI (or use
  the MCP `plan` tool) — it renders the templates with all current
  placeholders and shows you the exact YAML that would be applied.
- **Watch the Hoptimator operator logs** when iterating on templates. The
  operator logs the resolved template values just before applying them.
- **Keep `databases` explicit.** A template with `databases: null` matches
  *every* database and can produce surprising specs. List the database(s)
  it's meant for unless the template is genuinely universal.

---

## Configuration: where placeholder values come from

Templates are rendered against a configuration set assembled from three
sources, plus two sets of placeholders the deployer injects automatically.

### Default placeholders (deployer-injected)

These are always available without you setting them anywhere.

#### From `K8sSourceDeployer` (TableTemplate)

| Placeholder    | Value                                                                         |
| -------------- | ----------------------------------------------------------------------------- |
| `{{name}}`     | Kubernetes-safe name for this resource (typically `<database>-<table>` or similar). |
| `{{database}}` | The internal database name (e.g. `ads-database`).                              |
| `{{schema}}`   | The schema name (e.g. `ADS`).                                                  |
| `{{table}}`    | The unqualified table name.                                                    |
| `{{pipeline}}` | The pipeline this element belongs to.                                          |

#### From `K8sJobDeployer` (JobTemplate)

In addition to the above:

| Placeholder        | Value                                                                                            |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| `{{sql}}`          | The user's source SQL.                                                                           |
| `{{flinksql}}`     | The full auto-generated Flink SQL — including source/sink `CREATE TABLE` statements and the `INSERT INTO`. |
| `{{flinkconfigs}}` | Flink-specific configuration block, when applicable.                                             |

### The three configuration sources

#### 1. `hoptimator-configmap`

A namespace-scoped ConfigMap that ships with the bundled Kubernetes
deployment. The default `K8sConfigProvider` reads it on every connection
and exports its top-level keys as configuration values.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: hoptimator-configmap
data:
  # Top-level scalars become placeholder values directly.
  flink.parallelism: "2"
  flink.app.name: hoptimator-flink-runner

  # File-like keys can be expanded into multiple key=value pairs
  # by passing the key name to ConfigService.config(...).
  flink.config: |
    flink.app.name=hoptimator-flink-runner
    flink.app.type=SQL
```

The bundled
[`deploy/config/hoptimator-configmap.yaml`](https://github.com/linkedin/Hoptimator/blob/main/deploy/config/hoptimator-configmap.yaml)
is a starting point — replace its contents with values appropriate for your
environment.

Configmap values are the right home for **namespace-wide static
configuration**: cluster endpoints, default parallelism, image tags, all the
cross-pipeline knobs that don't belong baked into individual templates.

#### 2. JDBC connection properties (and `Subscription` hints)

Anything passed on the JDBC URL or set as a `Properties` entry on the
connection becomes a configuration value too. These are the right place
for **per-connection or per-pipeline overrides**:

```bash
./hoptimator -u "jdbc:hoptimator://k8s.namespace=my-team;hints=kafka.partitions=4"
```

For YAML-driven workflows, `Subscription.spec.hints` plays the same role:

```yaml
spec:
  hints:
    kafka.partitions: "4"
    flink.parallelism: "2"
```

See [Hints](../user-guide/hints.md) for the user-facing hint catalogue.

#### 3. JVM system properties

A fallback for values that don't fit in either of the above — typically
operational settings tied to the host process (debug ports, log levels,
proxy settings). Set them at JVM start:

```bash
java -DSELF_POD_NAMESPACE=my-team ...
```

The default `SystemPropertiesConfigProvider` exposes them.

### Precedence

When a placeholder is referenced, sources are consulted in this order
(later sources override earlier ones):

1. JVM system properties.
2. Configmap values (top-level keys, plus any expanded file-like keys).
3. Connection properties / hints.

In practice: **hints win, the configmap is the default, system properties
are a last resort.** A hint of `kafka.partitions=4` overrides whatever the
configmap says.

`{{var:default}}` lets a template fall back to a hard-coded default if no
source provides `var`. This is the right pattern for "sensible default,
overridable" knobs:

```yaml
partitions: {{kafka.partitions:1}}
parallelism: {{flink.parallelism:1}}
```

### File-like keys

A configmap value may be a multi-line block of key=value pairs (the
`flink.config` style above). These *do not* become placeholders directly —
they're parsed lazily, only when a caller asks for them by passing the key
as an `expansionFields` argument to `ConfigService.config(...)`.

If a template references `{{flink.app.type}}` and the only place that key
is set is inside the multi-line `flink.config` block, the corresponding
`Properties` need to be expanded by code that knows to ask for
`flink.config`. The default Flink job deployer does this, which is how the
bundled samples work.

For typical use, prefer top-level scalar keys — they Just Work.

### Connection properties reference

These are the properties the bundled Kubernetes deployer recognizes. They
can be set on the JDBC URL (`jdbc:hoptimator://k8s.namespace=my-team`),
passed in a `Properties` object to `DriverManager.getConnection`, or, in
the operator deployment, baked into the Calcite model file.

| Property                          | Default                          | Description                                                                              |
| --------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| `k8s.namespace`                   | the active namespace             | Namespace Hoptimator reads CRDs from and writes deployed resources to.                   |
| `k8s.watch.namespace`             | same as `k8s.namespace`          | Namespace the operator watches for reconciliation. Empty string means all namespaces.   |
| `k8s.kubeconfig`                  | `$KUBECONFIG` / `~/.kube/config` | Path to a kubeconfig file.                                                               |
| `k8s.server`                      | from kubeconfig                  | API server URL. Required with `k8s.token` or `k8s.user`+`k8s.password`.                  |
| `k8s.user` / `k8s.password`       | *(none)*                         | Basic-auth credentials. Requires `k8s.server`.                                           |
| `k8s.token`                       | *(none)*                         | Bearer token. Requires `k8s.server`.                                                     |
| `k8s.impersonate.user`            | *(none)*                         | Impersonate this user when calling the API.                                              |
| `k8s.impersonate.group`           | *(none)*                         | Single impersonation group.                                                              |
| `k8s.impersonate.groups`          | *(none)*                         | Comma-separated impersonation groups.                                                    |
| `k8s.ssl.truststore.location`     | *(none)*                         | Path to a PEM/JKS truststore for the API server certificate.                             |

If none of the above are set, the driver behaves like `kubectl` would: it
reads `~/.kube/config` and uses the active context.

These are deployer-specific. The driver-level connection properties
(`catalogs`, `hints`, `fun`) are documented on the
[JDBC driver](../user-guide/jdbc.md#connection-properties) page. Different
deployers would expose their own `<deployer>.*` properties.

### Pod-namespace detection

If `k8s.namespace` isn't set explicitly, the operator falls back to:

1. The contents of the file pointed to by `POD_NAMESPACE_FILEPATH` (the
   default Kubernetes `serviceAccount` mount populates this).
2. The `SELF_POD_NAMESPACE` JVM system property.
3. `default`.

The first path is the standard production setup — running inside a pod,
the serviceAccount mount supplies the namespace.

### Extending: writing a `ConfigProvider`

`ConfigProvider` is a Java SPI. Implement the interface, register the
implementation with `META-INF/services/com.linkedin.hoptimator.ConfigProvider`,
and Hoptimator will load it alongside the bundled providers (configmap,
system properties).

For mechanics and patterns, see
[Config providers](../extending/config-providers.md). Typical reasons to
write one: pulling values from a centralized secrets store, reading
per-environment defaults from an internal config service, or computing
values dynamically per-connection.

### Quick rules of thumb

- **Static, namespace-wide value** → configmap.
- **Per-pipeline / per-connection override** → hint or JDBC property.
- **Tied to the JVM, not the data** → system property.
- **Computed at runtime from external state** → custom `ConfigProvider`.
- **Sensible default that's only overridden occasionally** → ship the
  default in the template (`{{key:default}}`); let users override via hint.

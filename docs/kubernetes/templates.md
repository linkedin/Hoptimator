# Templates

`TableTemplate` and `JobTemplate` are how you tell Hoptimator what to deploy
when a table or job becomes part of a pipeline. They are the customization
workhorse — most adapters can be added without writing Java just by
authoring templates.

## How matching works

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

## Template structure

### TableTemplate

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
    apiVersion: kafka.strimzi.io/v1beta2
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

### JobTemplate

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

## Placeholder syntax

Placeholders are `{{...}}` substitutions resolved at render time. Their
behavior:

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

### Multi-line values in lists and comments

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

## Default placeholders

Two sets of placeholders are always available without you setting them.

### From `K8sSourceDeployer` (TableTemplate)

| Placeholder    | Value                                                                         |
| -------------- | ----------------------------------------------------------------------------- |
| `{{name}}`     | Kubernetes-safe name for this resource (typically `<database>-<table>` or similar). |
| `{{database}}` | The internal database name (e.g. `ads-database`).                              |
| `{{schema}}`   | The schema name (e.g. `ADS`).                                                  |
| `{{table}}`    | The unqualified table name.                                                    |
| `{{pipeline}}` | The pipeline this element belongs to.                                          |

### From `K8sJobDeployer` (JobTemplate)

In addition to the above:

| Placeholder        | Value                                                                                            |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| `{{sql}}`          | The user's source SQL.                                                                           |
| `{{flinksql}}`     | The full auto-generated Flink SQL — including source/sink `CREATE TABLE` statements and the `INSERT INTO`. |
| `{{flinkconfigs}}` | Flink-specific configuration block, when applicable.                                             |

### From the active `ConfigProvider`

Every key in `hoptimator-configmap` (or whatever your `ConfigProvider`
exposes) is available as a placeholder. See [Configuration](configuration.md)
for the loading order.

### From hints

Any [hint](../user-guide/hints.md) on the connection or `Subscription`
becomes a placeholder. This is the primary mechanism for per-pipeline
overrides:

```bash
./hoptimator -u "jdbc:hoptimator://hints=kafka.partitions=4,flink.parallelism=2"
```

The template can then reference `{{kafka.partitions}}` and
`{{flink.parallelism}}`. Hints take precedence over the configmap.

## Conditional templates: `{{var==value}}` / `{{var!=value}}`

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

## Authoring patterns

### One adapter, two templates

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

### Stacking templates

Multiple matching templates all contribute. To attach an auto-generated
`TableTrigger` to every Kafka source, ship a `TableTemplate` that emits a
`TableTrigger` resource alongside the Strimzi `KafkaTopic` from the main
template. See `ads-source-trigger-template` in
`deploy/samples/demodb.yaml`.

### Adapting an existing system without deploying anything new

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

## Tips

- **Test before you deploy.** Run `!specify <sql>` from the SQL CLI (or use
  the MCP `plan` tool) — it renders the templates with all current
  placeholders and shows you the exact YAML that would be applied.
- **Watch the Hoptimator operator logs** when iterating on templates. The
  operator logs the resolved template values just before applying them.
- **Keep `databases` explicit.** A template with `databases: null` matches
  *every* database and can produce surprising specs. List the database(s)
  it's meant for unless the template is genuinely universal.

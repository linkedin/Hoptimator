# CRD reference

Every custom resource Hoptimator installs, with field-by-field detail. All
CRDs are in the `hoptimator.linkedin.com/v1alpha1` API group. The `apiVersion`
is `v1alpha1` and **subject to change** — pin deliberately.

The CRD YAMLs live in
[`hoptimator-k8s/src/main/resources/`](https://github.com/linkedin/Hoptimator/tree/main/hoptimator-k8s/src/main/resources)
and are applied by `make deploy` along with the operator.

> **If you're modifying a CRD**, regenerate the Java model classes after
> your change:
>
> ```bash
> make generate-models
> ```
>
> The script invokes the upstream Kubernetes Java client's
> [`crd-model-gen`](https://github.com/kubernetes-client/java/tree/master/crd-model-gen)
> Docker image to produce the typed `V1alpha1*` classes the operator and
> deployers consume. Commit the regenerated files with your CRD change.

## At a glance

| Kind            | Plural          | Short names    | What it is                                                                                  |
| --------------- | --------------- | -------------- | ------------------------------------------------------------------------------------------- |
| `Database`      | `databases`     | `db`, `dbs`    | Registers an external system with the catalog (a JDBC URL + schema name).                   |
| `View`          | `views`         | —              | A logical SQL view.                                                                         |
| `Pipeline`      | `pipelines`     | `pip`, `pips`  | The deployable unit produced by a materialized view (sources, sink, job).                   |
| `TableTemplate` | `tabletemplates`| `tabt`         | Template for source/sink resource generation, scoped by database and access method.         |
| `JobTemplate`   | `jobtemplates`  | `jobt`         | Template for job resource generation, scoped by database.                                   |
| `TableTrigger`  | `tabletriggers` | —              | Fires a Job when an upstream table changes or on a cron schedule.                           |
| `Subscription`  | `subscriptions` | `sub`, `subs`  | YAML-native equivalent of `CREATE MATERIALIZED VIEW`.                                       |
| `LogicalTable`  | `logicaltables` | `lt`           | One named entity bound to multiple physical tier backends.                                  |
| `Engine`        | `engines`       | `eng`          | Registers a query-execution runtime. Optional; see [concepts](../getting-started/concepts.md#engines-optional). |
| `SqlJob`        | `sqljobs`       | `sql`, `sj`    | Primitive consumed by an `SqlJob` operator to deploy Flink or Flink-Beam SQL jobs.          |

---

## Database

Registers an external system in the catalog. The JDBC URL drives schema
discovery; the `schema` field is what the system shows up as in SQL.

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

### Spec fields

| Field      | Type   | Required | Description                                                                            |
| ---------- | ------ | :------: | -------------------------------------------------------------------------------------- |
| `url`      | string | yes      | JDBC connection URL.                                                                   |
| `schema`   | string |          | Schema name as rendered in the catalog (e.g. `ADS`).                                   |
| `catalog`  | string |          | JDBC catalog name. Used for hierarchical sources (e.g. MySQL).                         |
| `dialect`  | enum   |          | `ANSI`, `MySQL`, or `Calcite`. Affects how the planner generates SQL for this source.  |
| `driver`   | string |          | Fully qualified class name of the JDBC driver, if it isn't auto-discovered.            |

### Printer columns

`CATALOG`, `SCHEMA`, `URL`.

---

## View

A logical view. Each `CREATE VIEW` statement produces one of these. Views
are stored definitions; nothing is deployed to materialize them.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: View
metadata:
  name: ads-audience
spec:
  schema: ADS
  view: AUDIENCE
  sql: |
    SELECT FIRST_NAME, LAST_NAME
    FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS
  materialized: false
```

### Spec fields

| Field          | Type    | Required | Description                                                                |
| -------------- | ------- | :------: | -------------------------------------------------------------------------- |
| `view`         | string  | yes      | View name.                                                                 |
| `sql`          | string  | yes      | The view's SQL.                                                            |
| `schema`       | string  |          | Schema the view belongs to.                                                |
| `catalog`      | string  |          | Catalog the view belongs to.                                               |
| `materialized` | boolean |          | Whether the view should be materialized (i.e. paired with a `Pipeline`).   |

### Status fields

| Field       | Type      | Description                                                |
| ----------- | --------- | ---------------------------------------------------------- |
| `watermark` | date-time | Timestamp of the last data change event affecting this view. |

### Printer columns

`CATALOG`, `SCHEMA`, `VIEW`, `WATERMARK`.

---

## Pipeline

The deployable unit produced by a materialized view: a job plus its
sources and sink. The `sql` field is the auto-generated `INSERT INTO`
statement; the `yaml` field is the rendered specs of every component.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Pipeline
metadata:
  name: ads-audience
spec:
  sql: INSERT INTO `ADS`.`AUDIENCE` SELECT ...
  yaml: |
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkSessionJob
    ...
```

### Spec fields

| Field  | Type   | Description                                                                |
| ------ | ------ | -------------------------------------------------------------------------- |
| `sql`  | string | The `INSERT INTO` statement this pipeline implements.                      |
| `yaml` | string | The concatenated YAML of every object that makes up the pipeline.          |

### Status fields

| Field     | Type    | Description                                                       |
| --------- | ------- | ----------------------------------------------------------------- |
| `ready`   | boolean | Whether the entire pipeline is ready.                             |
| `failed`  | boolean | Whether any element of the pipeline has failed.                   |
| `message` | string  | Free-text status message — typically the last error, if any.      |
| `jobs`    | object  | Map of external jobs this pipeline triggers, with their state.    |

### Printer columns

`SQL`, `STATUS`.

---

## TableTemplate

Generates source/sink YAML and connector configs when a matching table
becomes part of a pipeline. The `databases` and `methods` fields gate which
tables a template applies to.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTemplate
metadata:
  name: kafka-template
spec:
  databases:
    - kafka-database
  methods:
    - Scan
  yaml: |
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaTopic
    metadata:
      name: {{name}}
    spec:
      topicName: {{table}}
      partitions: {{kafka.partitions:1}}
  connector: |
    connector = kafka
    topic = {{table}}
    properties.bootstrap.servers = ...
```

### Spec fields

| Field        | Type   | Description                                                                                       |
| ------------ | ------ | ------------------------------------------------------------------------------------------------- |
| `yaml`       | string | YAML template used to generate K8s specs. Placeholders are `{{var}}` ([syntax](templates.md#placeholder-syntax)).    |
| `connector`  | string | Java-properties-style template used to generate the engine's connector config.                    |
| `databases`  | array  | Database names this template matches. If null/empty, matches every database.                      |
| `methods`    | array  | Access methods to match: `Scan` (read), `Modify` (write). If null/empty, matches all.             |

A template can contribute `yaml`, `connector`, or both. A template that
provides only `connector` is useful for adapters that don't need to deploy
new infrastructure (the upstream resource already exists) but still need to
declare how to read or write it.

See [Templates and configuration](templates.md) for placeholder syntax and full examples.

---

## JobTemplate

Generates the YAML for a job (Flink session job, Beam runner, etc.) when a
pipeline targets a matching database.

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
```

### Spec fields

| Field       | Type   | Description                                                                          |
| ----------- | ------ | ------------------------------------------------------------------------------------ |
| `yaml`      | string | YAML template. Has access to `{{flinksql}}`, `{{flinkconfigs}}`, plus everything in [TableTemplate's environment](templates.md#default-placeholders-deployer-injected). |
| `databases` | array  | Database names this template matches. If null/empty, matches every database.         |

---

## TableTrigger

Runs a Kubernetes job when an upstream table changes or on a cron schedule.
See [Triggers](triggers.md) for operational guidance.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTrigger
metadata:
  name: refresh-audience
spec:
  schema: KAFKA
  table: existing-topic-1
  schedule: "@hourly"
  yaml: |
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: refresh-audience-job
    spec:
      template:
        spec:
          containers:
            - name: backfill
              image: ...
              command: [...]
          restartPolicy: Never
```

### Spec fields

| Field           | Type    | Required | Description                                                                                |
| --------------- | ------- | :------: | ------------------------------------------------------------------------------------------ |
| `schema`        | string  | yes      | Schema of the table the trigger watches (e.g. `KAFKA`).                                    |
| `table`         | string  | yes      | Table name the trigger watches.                                                            |
| `yaml`          | string  |          | The Job (or other resource) to (re)create when the trigger fires.                          |
| `jobProperties` | object  |          | Extra source-specific properties available to the job at runtime.                          |
| `schedule`      | string  |          | Cron schedule. If set, the trigger fires on a schedule. If null, it fires on status patches. |
| `paused`        | boolean |          | When true, the trigger does not fire (status updates are ignored).                         |

### Status fields

| Field       | Type      | Description                                                                  |
| ----------- | --------- | ---------------------------------------------------------------------------- |
| `timestamp` | date-time | When the trigger was last fired. **Patching this fires the trigger.**        |
| `watermark` | date-time | Timestamp of the last *successfully* processed event.                        |
| `jobs`      | object    | Per-job state — useful for tracking the status of jobs the trigger spawned.  |

### Printer columns

`PAUSED`, `SCHEMA`, `TABLE`, `SCHEDULE`, `TIMESTAMP`, `WATERMARK`.

---

## Subscription

YAML-native way to declare a materialized view. Equivalent to running
`CREATE MATERIALIZED VIEW <database>.<sink> AS <sql>` against the JDBC
driver, but useful in GitOps workflows where pipelines should live next to
the rest of your manifests.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Subscription
metadata:
  name: my-feature
spec:
  database: VENICE
  sql: |
    SELECT m.id AS KEY, m.first_name FROM PROFILE.MEMBERS m
  hints:
    flink.parallelism: "2"
```

### Spec fields

| Field      | Type   | Required | Description                                                                       |
| ---------- | ------ | :------: | --------------------------------------------------------------------------------- |
| `sql`      | string | yes      | A single SQL query.                                                               |
| `database` | string | yes      | The database in which to create the output sink table.                            |
| `hints`    | object |          | Hints to adapters and the planner. May be ignored if a different plan is chosen.  |

### Status fields

| Field                 | Type    | Description                                                            |
| --------------------- | ------- | ---------------------------------------------------------------------- |
| `ready`               | boolean | Whether the subscription is ready to be consumed.                      |
| `failed`              | boolean | Whether the operator was unable to deploy a pipeline.                  |
| `message`             | string  | Free-text status, typically the last error.                            |
| `sql`                 | string  | The SQL the pipeline ended up implementing (may be planner-rewritten). |
| `hints`               | object  | The hints that survived planning.                                      |
| `attributes`          | object  | Physical attributes of the job and sink/output table.                  |
| `resources`           | array   | All YAML generated to implement the pipeline.                          |
| `jobResources`        | array   | YAML for the job specifically.                                         |
| `downstreamResources` | array   | YAML for the sink/output table.                                        |

### Printer columns

`STATUS`, `DB`, `SQL`.

---

## LogicalTable

One named entity backed by multiple physical tiers. See
[Logical tables in concepts](../getting-started/index.md) for the bigger
picture.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: LogicalTable
metadata:
  name: audience
spec:
  tableName: audience
  tiers:
    nearline:
      database: kafka-database
    online:
      database: venice
    offline:
      database: hdfs-database
```

### Spec fields

| Field       | Type   | Description                                                                            |
| ----------- | ------ | -------------------------------------------------------------------------------------- |
| `tableName` | string | Original table name as declared in `CREATE TABLE` (e.g. `audience`).                   |
| `tiers`     | object | Map of tier name (`nearline`, `online`, `offline`) to a tier binding.                  |

Each tier binding has one field:

| Field      | Type   | Required | Description                                       |
| ---------- | ------ | :------: | ------------------------------------------------- |
| `database` | string | yes      | Name of the `Database` CRD backing this tier.     |

The `LogicalTableDeployer` runs at create time to deploy physical tier
resources, the implicit inter-tier sync pipelines, and the offline-tier
backfill trigger when an offline tier is bound.

---

## Engine (optional)

Registers a query-execution runtime. See
[Engines in concepts](../getting-started/concepts.md#engines-optional)
for what this surface is and isn't — short version: pipeline
materialization does *not* require an `Engine` resource.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: Engine
metadata:
  name: flink
spec:
  url: jdbc:flink://gateway.example/
  dialect: Flink
```

### Spec fields

| Field       | Type   | Required | Description                                                            |
| ----------- | ------ | :------: | ---------------------------------------------------------------------- |
| `url`       | string | yes      | JDBC URL Hoptimator uses to submit queries to the engine.              |
| `dialect`   | enum   |          | `ANSI` or `Flink`.                                                     |
| `driver`    | string |          | Fully qualified JDBC driver class name.                                |
| `databases` | array  |          | Databases this engine supports. If null/empty, supports all of them.   |

### Printer columns

`URL`.

---

## SqlJob

A declarative spec for a SQL job — Flink or Flink-Beam, streaming or
batch — that an `SqlJob` operator picks up and deploys. Hoptimator itself
doesn't reconcile `SqlJob` resources; an external operator paired with this
CRD does.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: SqlJob
metadata:
  name: my-sql-job
spec:
  dialect: Flink
  executionMode: Streaming
  sql:
    - "CREATE TABLE input ... WITH ('connector' = 'kafka', ...);"
    - "CREATE TABLE output ... WITH ('connector' = 'venice', ...);"
    - "INSERT INTO output SELECT * FROM input;"
  configs:
    flink.parallelism: "4"
```

### Spec fields

| Field           | Type   | Required | Description                                                                       |
| --------------- | ------ | :------: | --------------------------------------------------------------------------------- |
| `sql`           | array  | yes      | One or more SQL statements run as a single job.                                   |
| `dialect`       | enum   |          | `Flink` (default) or `FlinkBeam`.                                                 |
| `executionMode` | enum   |          | `Streaming` (default) or `Batch`.                                                 |
| `configs`       | object |          | Job-level configuration passed through to the engine.                             |

### Status fields

| Field     | Type    | Description                                                       |
| --------- | ------- | ----------------------------------------------------------------- |
| `ready`   | boolean | Whether the job is running or has completed.                      |
| `failed`  | boolean | Whether the job has failed.                                       |
| `message` | string  | Status message — typically the last error if any.                 |
| `sql`     | string  | The SQL the operator is implementing.                             |
| `configs` | object  | The configs the operator is using.                                |

### Printer columns

`DIALECT`, `MODE`, `STATUS`.

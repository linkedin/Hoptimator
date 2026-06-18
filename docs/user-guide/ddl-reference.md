# DDL reference

Hoptimator's SQL is Calcite-based. SELECTs and most expressions follow Calcite
ANSI SQL — see the
[Calcite reference](https://calcite.apache.org/docs/reference.html) for those.
This page documents the DDL Hoptimator adds on top.

> All DDL listed here also has a YAML equivalent: a `View`, `Pipeline`,
> `TableTrigger`, etc. CRD. Use whichever is more ergonomic for your workflow.

> Test cases for the DDL parser and executor live as
> [Quidem](https://github.com/julianhyde/quidem) `.id` scripts under each
> module's `src/test/resources/` (e.g. `basic-ddl.id`, `k8s-ddl.id`).
> Reading them is a fast way to see currently-passing examples of every
> DDL form.

## CREATE semantics: strict vs. apply mode

`CREATE` has two flavors, selected per-connection via the `mode` property:

| Mode               | `CREATE`                                | `CREATE OR REPLACE`  |
|--------------------|-----------------------------------------|----------------------|
| `create` (default) | Fail if the resource exists.            | Update if it exists. |
| `apply`            | Converge the resource to the definition. Idempotent. | Same as `CREATE`. |

`create` mode is the imperative default — good for interactive sessions where
you want a real error if you accidentally redefine something.

`apply` mode is the declarative, K8s-style flavor: a `.sql` file becomes a
manifest, and re-running it converges resources to the declared state. It's
designed for check-in-and-reconcile workflows where CI runs the same script
on every merge. Set it on the JDBC URL:

```
jdbc:hoptimator://...;mode=apply
```

No `CREATE` form ever deletes anything. In both modes, `CREATE` — including
`CREATE OR REPLACE` and apply-mode convergence — only creates or updates
resources; it never drops the target or its backing data store. This is the
safety guarantee that makes apply mode usable in production. (`DROP` is the
only form that removes resources, and it is imperative and explicit — see
[DROP](#drop). Depending on the deployer, `DROP` may delete the underlying
data store, e.g. a Kafka topic or MySQL table.)

**Out of scope today:** prune-by-absence (the K8s `--prune` equivalent — a
resource missing from the declared script does *not* trigger a drop). `DROP`
remains imperative and explicit. Detection of *incompatible* metadata changes
(e.g. dropping a primary key) is also not yet wired in; in apply mode both
`CREATE` and `CREATE OR REPLACE` apply the new definition regardless. Use
caution when changing schemas of in-flight pipelines.

## Dry-run: validating a script without deploying

The `deploy` connection property toggles whether DDL actually touches the
underlying deployers.

| `deploy` value     | Behavior                                                                    |
|--------------------|-----------------------------------------------------------------------------|
| `true` (default)   | Normal operation. Each DDL invokes the deployers (create/update/delete).    |
| `false`            | Dry-run. Each DDL is parsed, validated, and applied to the in-memory schema, but no deployer is invoked. |

```
jdbc:hoptimator://...;deploy=false
```

In dry-run mode, a session can execute a multi-statement script end-to-end —
later statements see the in-memory effects of earlier ones (a `CREATE TABLE
FOO` followed by `CREATE VIEW BAR AS SELECT * FROM FOO` validates correctly),
and `DROP` removes its target from the in-memory schema so a follow-up
`CREATE` of the same name doesn't collide. Nothing reaches the deployers.

`deploy=false` is orthogonal to `mode`: combining it with `mode=apply`
dry-runs an apply-mode script.

> Dry-run is distinct from `!specify` (and the underlying `SPECIFY` mode;
> see [CLI reference](sql-cli.md#specify-sql)),
> which is the strict zero-side-effect preview used to render deployment
> artifacts for a single statement. `!specify` always invokes
> `deployer.specify()` and unwinds the in-memory schema afterward. Dry-run
> preserves the in-memory mutation across statements and invokes no deployer
> method at all.

## CREATE VIEW

```
CREATE [OR REPLACE] VIEW <name> [(<column>, ...)] AS <query>
```

Defines a logical view. The query is rewritten when referenced; nothing is
deployed.

```sql
CREATE OR REPLACE VIEW ADS.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;
```

Each `CREATE VIEW` produces a `View` Kubernetes resource.

## CREATE MATERIALIZED VIEW

```
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] <name>
  [(<column>, ...)]
  [REFRESHED '<cron>']
  [WITH ('<key>' '<value>', ...)]
AS <query>
```

Defines a view *and* deploys a running pipeline that maintains its sink.

- The unqualified leaf of `<name>` is the sink table; the schema of `<name>`
  selects the database the sink lives in. The leaf may use the
  [partial-view syntax](#partial-views-multiple-pipelines-into-one-sink) to
  share a sink across many materialized views.
- `REFRESHED '<cron>'` is reserved for batch refreshes; today it is honored by
  job templates that opt in to it.
- `WITH (...)` accepts arbitrary key/value options that flow through to the
  templates as overrides.

```sql
CREATE MATERIALIZED VIEW VENICE."AUDIENCE$members" AS
  SELECT MEMBER_URN AS "KEY", FIRST_NAME
  FROM PROFILE.MEMBERS;
```

The result is a `View` *and* a `Pipeline` resource, plus all of the engine and
connector resources their templates produce. To preview before deploying, use
`!specify` ([CLI](sql-cli.md#specify-sql)) or the `plan` MCP tool
([MCP](mcp-server.md#planning-read-only-but-expensive)).

### Partial views (multiple pipelines into one sink)

The view name accepts an optional `$<suffix>` segment. When present, the part
before the `$` is the **sink table** and the part after is just a unique
suffix for the pipeline. Multiple materialized views can share the same
sink — use the suffix to give each one a distinct name.

```sql
-- Both write into the same VENICE.AUDIENCE sink,
-- but they are separate pipelines.
CREATE MATERIALIZED VIEW VENICE."AUDIENCE$members" AS
  SELECT MEMBER_URN AS "KEY", first_name FROM PROFILE.MEMBERS;

CREATE MATERIALIZED VIEW VENICE."AUDIENCE$articles" AS
  SELECT id AS "KEY", title FROM CONTENT.ARTICLES;
```

The pipeline name becomes `<database>-<sink>-<suffix>` so the deployed
resources don't collide. The sink itself only gets created once; subsequent
`CREATE`s use the existing row type.

This pattern is the **recommended default** for most production cases:

- Many real sinks (Venice stores, Kafka topics, Pinot tables) are populated
  by several distinct pipelines that each contribute different rows or
  fields. A partial view is the natural way to express that.
- Without `$`, two `CREATE MATERIALIZED VIEW` statements targeting the same
  schema/table would conflict at deploy time.
- The partial-view name shows up in `kubectl get pipelines` so you can keep
  many writers to the same sink visible and individually manageable.

Use a non-partial name (`VENICE.AUDIENCE`) only when you genuinely have one
pipeline owning one sink end-to-end.

## DROP

```
DROP TABLE [IF EXISTS] <name>
DROP VIEW [IF EXISTS] <name>
DROP MATERIALIZED VIEW [IF EXISTS] <name>
DROP TRIGGER [IF EXISTS] <name>
```

Each form removes the corresponding Kubernetes resource and (where
applicable) the pipeline behind it. `DROP MATERIALIZED VIEW` tears down the
job, the sink, and any intermediate hops the planner created.

## CREATE TRIGGER

```
CREATE [OR REPLACE] TRIGGER [IF NOT EXISTS] <name>
  ON <schema.table>
  AS '<yaml-template>'
  [IN '<namespace>']
  [SCHEDULED '<cron>']
  [WITH ('<key>' '<value>', ...)]
```

Equivalent to a `TableTrigger` CRD: runs the embedded YAML (typically a Job
or CronJob) when the named table changes or on a cron schedule. The job spec
is arbitrary, so triggers are how you wire up backfills, rETL refreshes,
downstream notifications, and operational hooks without embedding that
logic in the pipeline itself. See
[TableTriggers in concepts](../getting-started/concepts.md#tabletriggers)
for the bigger picture.

```sql
CREATE TRIGGER refresh_audience
  ON KAFKA.existing-topic-1
  AS 'apiVersion: batch/v1
       kind: Job
       ...'
  SCHEDULED '@hourly';
```

Pause and resume:

```sql
PAUSE TRIGGER refresh_audience;
RESUME TRIGGER refresh_audience;
```

## CREATE TABLE

```
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <name>
  (<column> <type>, ...)
  [WITH ('<key>' '<value>', ...)]

CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <name> LIKE <source>
```

Provisions a real table in the database identified by `<name>`'s schema.
Unlike vanilla SQL, this isn't just a metadata operation: the planner runs
the resulting `Source` through the same **Deployer SPI** that backs
materialized-view sources and sinks, so the underlying infrastructure is
created on demand.

For example, against a Kafka adapter, `CREATE TABLE KAFKA.my_topic (...)`
asks the Kafka deployer to create the topic itself — no separate Strimzi
manifest, no operator round-trip. Against the Venice adapter it asks for a
new store. Against `demodb` it's a no-op because the source is in-memory.
Each Deployer contains its own set of configuration options.

```sql
CREATE TABLE KAFKA.audience (
  KEY VARCHAR,
  FIRST_NAME VARCHAR,
  LAST_NAME VARCHAR
) WITH ('kafka.partitions' '8');
```

Once the table exists, materialized views can write to it via
[partial views](#partial-views-multiple-pipelines-into-one-sink):

```sql
CREATE MATERIALIZED VIEW KAFKA."audience$members" AS
  SELECT MEMBER_URN AS KEY, FIRST_NAME, LAST_NAME FROM PROFILE.MEMBERS;
```

Populating a new table from a query (`CREATE TABLE ... AS <query>`) is not
supported today.


## Identifiers and case sensitivity

- Unquoted identifiers fold to upper case (`page_views` → `PAGE_VIEWS`).
- Use double quotes for case-sensitive names (`"PageViewEvent"`).
- String literals use single quotes (`'value'`).
- Nested struct access uses bracket syntax: `"profile"['first_name']`.

## Function library

Hoptimator's CLI launches with `fun=mysql` so MySQL-style functions
(`CONCAT`, `SUBSTRING`, etc.) are recognized. To use a different library
(`oracle`, `postgresql`), pass it on the JDBC URL — see
[JDBC driver](jdbc.md).

## System tables

Hoptimator exposes its own state through a `k8s` schema. Useful for
introspecting deployed pipelines without leaving SQL:

```sql
SELECT * FROM "k8s".pipelines;
SELECT name, ready, failed, message FROM "k8s".pipeline_elements;
SELECT * FROM "k8s".views WHERE schema = 'ADS';
```

These tables are queryable; they are not writable.

## What's not supported

Two flavors: **categorically out of scope** and **reserved syntax that
parses but is a no-op today**. The latter may be activated in a future
version; if you depend on any of it, file an issue rather than relying on
the parse alone.

**Out of scope:**

- `INSERT` / `UPDATE` / `DELETE` against arbitrary tables — these are
  reserved for what the planner emits internally.
- `ALTER TABLE` — drop and recreate, or rely on the deployer's update
  path (`CREATE OR REPLACE`).
- Transactions. Each statement is auto-committed; rollbacks are not
  supported.
- Stored procedures.

**Parses but not yet executed:**

- `REFRESH MATERIALIZED VIEW <name>` — intended to re-run a batch-style
  materialization on demand.
- `FIRE TABLE | TRIGGER | VIEW | MATERIALIZED VIEW <name>` — intended to
  manually fire a side effect (e.g. for testing without waiting for a
  schedule).
- `PAUSE MATERIALIZED VIEW <name>` / `RESUME MATERIALIZED VIEW <name>` —
  parser support exists; executor does not. (`PAUSE TRIGGER` /
  `RESUME TRIGGER` above are fully implemented.)
- `CREATE [OR REPLACE] FUNCTION ...` — parser support exists for
  registering user-defined job templates; executor does not.

# DDL reference

Hoptimator's SQL is Calcite-based. SELECTs and most expressions follow Calcite
ANSI SQL — see the
[Calcite reference](https://calcite.apache.org/docs/reference.html) for those.
This page documents the DDL Hoptimator adds on top.

> All DDL listed here also has a YAML equivalent: a `View`, `Pipeline`,
> `TableTrigger`, etc. CRD. Use whichever is more ergonomic for your workflow.

## CREATE VIEW

```
CREATE [OR REPLACE] VIEW <name> [(<column>, ...)] AS <query>
```

Defines a logical view. The query is rewritten when referenced; nothing is
deployed.

```sql
CREATE OR REPLACE VIEW MY.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;
```

Each `CREATE VIEW` produces a `View` Kubernetes resource.

## CREATE MATERIALIZED VIEW

```
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] <name>
  [(<column>, ...)]
  [REFRESHED '<cron>']
  [WITH (<key> = <value>, ...)]
AS <query>
```

Defines a view *and* deploys a running pipeline that maintains its sink.

- The unqualified leaf of `<name>` is the sink table; the schema of `<name>`
  selects the database the sink lives in.
- `REFRESHED '<cron>'` is reserved for batch refreshes; today it is honored by
  job templates that opt in to it.
- `WITH (...)` accepts arbitrary key/value options that flow through to the
  templates as overrides.

```sql
CREATE MATERIALIZED VIEW VENICE."AUDIENCE$members" AS
  SELECT m.id AS "KEY", m.first_name
  FROM PROFILE.MEMBERS m;
```

The result is a `View` *and* a `Pipeline` resource, plus all of the engine and
connector resources their templates produce. To preview before deploying, use
`!specify` ([CLI](sql-cli.md#specify-sql)) or the `plan` MCP tool
([MCP](mcp-server.md#planning-read-only-but-expensive)).

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
  [WITH (<key> = <value>, ...)]
```

Equivalent to a `TableTrigger` CRD: runs the embedded YAML (typically a Job
or CronJob) when the named table changes or on a cron schedule.

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
  [(<element>, ...)]
  [WITH (<key> = <value>, ...)]
  [AS <query>]

CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <name> LIKE <source>
```

Defines a table in the database identified by `<name>`'s schema. Whether the
underlying database supports `CREATE TABLE` depends on its adapter — for
read-only sources it will fail.

## Reserved syntax (parses but not yet executed)

The grammar accepts a handful of statements whose executors are not yet
wired up. They parse cleanly but are no-ops today; treat them as reserved
syntax that future versions may activate.

- `REFRESH MATERIALIZED VIEW <name>` — intended to re-run a batch-style
  materialization on demand. Not implemented.
- `FIRE TABLE | TRIGGER | VIEW | MATERIALIZED VIEW <name>` — intended to
  manually fire a side effect (e.g. for testing without waiting for a
  schedule). Not implemented.
- `PAUSE MATERIALIZED VIEW <name>` / `RESUME MATERIALIZED VIEW <name>` —
  parser support exists; executor does not. (`PAUSE TRIGGER` /
  `RESUME TRIGGER`, above, are fully implemented.)
- `CREATE [OR REPLACE] FUNCTION ...` — parser support exists for
  registering user-defined job templates; executor does not.

If you depend on any of these, file an issue rather than relying on the
parse alone.

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
SELECT * FROM "k8s".views WHERE schema = 'MY';
```

These tables are queryable; they are not writable.

## What is *not* supported

- `INSERT` / `UPDATE` / `DELETE` against arbitrary tables — these are reserved
  for what the planner emits internally.
- `ALTER TABLE` — drop and recreate, or rely on the deployer's update path
  (`CREATE OR REPLACE`).
- Transactions. Each statement is auto-committed; rollbacks are not
  supported.
- Stored procedures.

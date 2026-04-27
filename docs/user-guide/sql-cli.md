# SQL CLI

`./hoptimator` is the interactive shell — a wrapper around
[sqlline](https://github.com/julianhyde/sqlline) connected to
`jdbc:hoptimator://`. It's the fastest way to explore the catalog, see what a
plan looks like, and create or drop pipelines.

## Launch

From the repo root, after `make build install`:

```bash
./hoptimator
```

The script wraps `sqlline.SqlLine` with Hoptimator's app config, which
registers a few extra commands on top of the standard sqlline ones.

You can pass any sqlline argument through. For example, run a script and exit:

```bash
./hoptimator --run=script.sql
```

To override the JDBC URL, pass `-u`:

```bash
./hoptimator -u "jdbc:hoptimator://k8s.namespace=my-team"
```

See [JDBC driver](jdbc.md) for the full URL syntax.

## Built-in commands

Standard sqlline commands all work — `!help`, `!tables`, `!schemas`, `!quit`,
`!run`, `!record`, etc. On top of those, Hoptimator adds:

| Command       | What it does                                                                  |
| ------------- | ----------------------------------------------------------------------------- |
| `!intro`      | One-screen tour. Run this first.                                              |
| `!resolve`    | Print the schema and source/sink connector configs Hoptimator would use for a table. |
| `!pipeline`   | Print the auto-generated pipeline SQL for a SELECT or CREATE MATERIALIZED VIEW statement. |
| `!specify`    | Print every Kubernetes spec the statement would deploy. The dry-run for `CREATE MATERIALIZED VIEW`. |

`!resolve`, `!pipeline`, and `!specify` do not modify any state. Use them to
sanity-check a plan before you let the JDBC driver actually deploy it.

### `!resolve <schema.table>`

```sql
0: Hoptimator> !resolve ADS.PAGE_VIEWS
Avro schema:
{ "type": "record", ... }

Source configs:
{connector=datagen, number-of-rows=10, ...}

Sink configs:
{connector=blackhole, ...}
```

Useful for confirming that your TableTemplates are picking up correctly and
producing the connector config you expect.

### `!pipeline <sql>`

```sql
0: Hoptimator> !pipeline CREATE MATERIALIZED VIEW MY.AUDIENCE AS
                          SELECT FIRST_NAME, LAST_NAME
                          FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;

CREATE TABLE `ADS-PAGE_VIEWS` (...) WITH (...);
CREATE TABLE `PROFILE-MEMBERS` (...) WITH (...);
CREATE TABLE `MY-AUDIENCE` (...) WITH (...);
INSERT INTO `MY-AUDIENCE`
  SELECT ... FROM `ADS-PAGE_VIEWS` JOIN `PROFILE-MEMBERS` ...
```

This is the literal SQL the engine (Flink, today) will run.

### `!specify <sql>`

```sql
0: Hoptimator> !specify CREATE MATERIALIZED VIEW MY.AUDIENCE AS
                         SELECT FIRST_NAME, LAST_NAME
                         FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;

apiVersion: hoptimator.linkedin.com/v1alpha1
kind: View
metadata:
  name: my-audience
...

---

apiVersion: flink.apache.org/v1beta1
kind: FlinkSessionJob
metadata:
  name: my-audience
spec:
  ...
```

If you'd `kubectl apply` the output, you'd get the same result as actually
running the `CREATE MATERIALIZED VIEW`. This is the safest way to review what
a statement will do before you run it.

## Running SQL

Hoptimator supports the SQL surface described in
[DDL reference](ddl-reference.md). The headline operations:

```sql
-- Read from any registered table
SELECT * FROM ADS.PAGE_VIEWS LIMIT 5;

-- Define a reusable view (no pipeline)
CREATE VIEW MY.AUDIENCE AS
  SELECT * FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;

-- Define a materialized view (creates a pipeline)
CREATE MATERIALIZED VIEW MY.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;

-- Drop either
DROP VIEW MY.AUDIENCE;
DROP MATERIALIZED VIEW MY.AUDIENCE;
```

Identifiers are case-sensitive when quoted with double quotes
(`"PageViewEvent"`). Unquoted identifiers fold to upper case in the default
configuration.

## Connecting to a different Kubernetes context

The CLI uses your active kubeconfig context by default — whatever
`kubectl config current-context` reports. To target a specific namespace, set
it on the JDBC URL:

```bash
./hoptimator -u "jdbc:hoptimator://k8s.namespace=my-team"
```

To use a non-default kubeconfig file:

```bash
./hoptimator -u "jdbc:hoptimator://k8s.kubeconfig=/path/to/config"
```

For the full list of `k8s.*` connection properties (server, token, user,
truststore, impersonation), see [JDBC driver](jdbc.md).

## Tips

- **Multi-line statements** — sqlline waits for a trailing semicolon, so feel
  free to break long DDL across lines.
- **Use `!record` to capture a session.** Useful when filing bug reports.
- **Use `!run` to re-play a script.** The integration tests do this; you can
  too.
- **Tab completion** is available for SQL keywords and registered table
  names — sqlline picks them up automatically once a connection is open.

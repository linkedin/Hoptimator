# JDBC driver

`hoptimator-jdbc-driver` is a JDBC `Driver` that lets any JVM application
connect to Hoptimator via the standard `java.sql.*` API. The same driver
underlies the [SQL CLI](sql-cli.md) and the [MCP server](mcp-server.md).

## Connection URL

Format:

```
jdbc:hoptimator://[<key>=<value>(;<key>=<value>)*]
```

Empty parameter list is fine — `jdbc:hoptimator://` connects with all
defaults.

The driver class is `com.linkedin.hoptimator.jdbc.HoptimatorDriver`. With the
driver jar on the classpath it self-registers via `DriverManager`, but you can
load it explicitly:

```java
Class.forName("com.linkedin.hoptimator.jdbc.HoptimatorDriver");
Connection conn = DriverManager.getConnection("jdbc:hoptimator://");
```

## Connection properties

Properties set in the URL (after `://`) and properties passed via a
`Properties` object to `getConnection` are merged; URL properties win.

### Catalog selection

| Property    | Default         | Description                                                                                |
| ----------- | --------------- | ------------------------------------------------------------------------------------------ |
| `catalogs`  | *all available* | Comma-separated list of catalog providers to load (`util,k8s,…`). Useful for tests.        |

### Hints

| Property | Default | Description                                                                                                                              |
| -------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `hints`  | *(none)* | Comma-separated `key=value` pairs passed to templates and connectors. URL-encode values that contain commas or `=`. See [hints](hints.md). |

### Kubernetes context

The default deployer talks to a Kubernetes cluster. These properties control
which one and how:

| Property                          | Default                          | Description                                                                              |
| --------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------- |
| `k8s.namespace`                   | the active namespace             | Namespace where Hoptimator reads CRDs and writes deployed resources.                     |
| `k8s.watch.namespace`             | same as `k8s.namespace`          | Namespace the operator watches for reconciliation.                                       |
| `k8s.kubeconfig`                  | `$KUBECONFIG` / `~/.kube/config` | Path to a kubeconfig file.                                                               |
| `k8s.server`                      | from kubeconfig                  | API server URL. Required when using `k8s.user`+`k8s.password` or `k8s.token`.            |
| `k8s.user` / `k8s.password`       | *(none)*                         | Basic-auth credentials. Requires `k8s.server`.                                           |
| `k8s.token`                       | *(none)*                         | Bearer token. Requires `k8s.server`.                                                     |
| `k8s.impersonate.user`            | *(none)*                         | Impersonate this user when calling the API.                                              |
| `k8s.impersonate.group`           | *(none)*                         | Single impersonation group.                                                              |
| `k8s.impersonate.groups`          | *(none)*                         | Comma-separated impersonation groups.                                                    |
| `k8s.ssl.truststore.location`     | *(none)*                         | Path to a PEM/JKS truststore for the API server certificate.                             |

If none of the above are set, the driver behaves like `kubectl` would: it
reads `~/.kube/config` and uses the active context.

### SQL function dialect

| Property | Default   | Description                                                                            |
| -------- | --------- | -------------------------------------------------------------------------------------- |
| `fun`    | *(Calcite default)* | Selects which Calcite SQL function library to enable (`mysql`, `oracle`, etc.). |

The default `./hoptimator` script uses `fun=mysql`.

### Anything else

Unknown properties are passed through to the underlying Calcite connection,
which means most Calcite [`CalciteConnectionProperty`](https://calcite.apache.org/javadocAggregate/org/apache/calcite/config/CalciteConnectionProperty.html)
values also work.

## Java example

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class HoptimatorExample {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty("k8s.namespace", "my-team");
    props.setProperty("hints", "kafka.partitions=4,flink.parallelism=2");

    try (Connection conn = DriverManager.getConnection("jdbc:hoptimator://", props);
         Statement stmt = conn.createStatement()) {

      // Run a query
      try (var rs = stmt.executeQuery("SELECT FIRST_NAME FROM PROFILE.MEMBERS LIMIT 5")) {
        while (rs.next()) {
          System.out.println(rs.getString(1));
        }
      }

      // Deploy a pipeline
      stmt.execute("""
          CREATE MATERIALIZED VIEW MY.AUDIENCE AS
            SELECT FIRST_NAME, LAST_NAME
            FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS
          """);
    }
  }
}
```

## Inspecting the catalog programmatically

The driver implements `DatabaseMetaData`, so standard JDBC introspection works:

```java
DatabaseMetaData meta = conn.getMetaData();
try (var rs = meta.getTables(null, null, "%", null)) {
  while (rs.next()) {
    System.out.println(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
  }
}
```

The MCP server uses exactly this pattern internally.

## Inspecting pipelines via SQL

Hoptimator exposes its own state through a built-in `k8s` schema. Useful for
dashboards, agents, or shell one-liners:

```sql
SELECT * FROM "k8s".pipelines;
SELECT name, ready, failed, message FROM "k8s".pipeline_elements WHERE name = 'my-audience';
```

## Depending on the driver

Pre-release artifacts are published to LinkedIn's
[JFrog Artifactory](https://linkedin.jfrog.io/artifactory/hoptimator). The
project is **alpha** — APIs and the driver's behavior may change between
versions, so pin deliberately.

For Gradle, add the JFrog repo and depend on `hoptimator-jdbc-driver`:

```groovy
repositories {
  maven { url 'https://linkedin.jfrog.io/artifactory/hoptimator' }
}

dependencies {
  implementation 'com.linkedin.hoptimator:hoptimator-jdbc-driver:<version>'
}
```

If you need only the core JDBC functionality, `hoptimator-jdbc` is the lower-
level module. The `-driver` artifact wraps it with the registration plumbing
needed for `DriverManager` discovery.

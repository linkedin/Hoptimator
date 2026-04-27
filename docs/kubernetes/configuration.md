# Configuration

Hoptimator pulls configuration values from three places: a Kubernetes
ConfigMap, JDBC connection properties (or `Subscription` hints), and JVM
system properties. This page documents what comes from where, the
precedence between them, and how to extend the surface with your own
`ConfigProvider`.

## The three sources

### 1. `hoptimator-configmap`

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

### 2. JDBC connection properties (and `Subscription` hints)

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

See [Hints](../user-guide/hints.md) for the full hint catalogue and
[JDBC driver](../user-guide/jdbc.md) for the connection-property reference.

### 3. JVM system properties

A fallback for values that don't fit in either of the above — typically
operational settings tied to the host process (debug ports, log levels,
proxy settings). Set them at JVM start:

```bash
java -DSELF_POD_NAMESPACE=my-team ...
```

The default `SystemPropertiesConfigProvider` exposes them.

## Precedence

When a placeholder is referenced, sources are consulted in this order
(later sources override earlier ones):

1. JVM system properties.
2. Configmap values (top-level keys, plus any expanded file-like keys).
3. Connection properties / hints.

In practice this means: **hints win, the configmap is the default, system
properties are a last resort.** A hint of `kafka.partitions=4` overrides
whatever the configmap says.

## How values flow into templates

Both `TableTemplate` and `JobTemplate` resolve `{{var}}` placeholders
against the merged configuration set. The deployer also injects its own
defaults — `{{name}}`, `{{table}}`, `{{flinksql}}`, etc. — which are *not*
sourced from configuration; see
[Templates → Default placeholders](templates.md#default-placeholders) for
that list.

`{{var:default}}` lets a template fall back to a hard-coded default if no
source provides `var`. This is the right pattern for "sensible default,
overridable" knobs:

```yaml
partitions: {{kafka.partitions:1}}
parallelism: {{flink.parallelism:1}}
```

## File-like keys

A configmap value may be a multi-line block of key=value pairs (the
`game.properties` / `flink.config` style at the top of this page). These
*do not* become placeholders directly — they're parsed lazily, only when a
caller asks for them by passing the key as an `expansionFields` argument to
`ConfigService.config(...)`.

If a template references `{{flink.app.type}}` and the only place that key
is set is inside the multi-line `flink.config` block, the corresponding
`Properties` need to be expanded by code that knows to ask for
`flink.config`. The default Flink job deployer does this, which is how the
bundled samples work.

For typical use, prefer top-level scalar keys — they Just Work.

## Connection properties reference

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

## Pod-namespace detection

If `k8s.namespace` isn't set explicitly, the operator falls back to:

1. The contents of the file pointed to by `POD_NAMESPACE_FILEPATH` (the
   default Kubernetes `serviceAccount` mount populates this).
2. The `SELF_POD_NAMESPACE` JVM system property.
3. `default`.

The first path is the standard production setup — running inside a pod, the
serviceAccount mount supplies the namespace.

## Extending: writing a `ConfigProvider`

`ConfigProvider` is a Java SPI. Implement the interface, register the
implementation with `META-INF/services/com.linkedin.hoptimator.ConfigProvider`,
and Hoptimator will load it alongside the bundled providers (configmap,
system properties).

```java
public class MyConfigProvider implements ConfigProvider {
  @Override
  public Properties loadConfig(Connection connection) throws SQLException {
    Properties p = new Properties();
    // populate p from your source of truth
    return p;
  }
}
```

Use cases that come up often:

- Reading from a centralized secrets store rather than a configmap.
- Pulling per-environment defaults from an internal config service.
- Computing values dynamically per-connection (e.g. inferring a region
  from the cluster's labels).

The order in which `ConfigProvider`s are loaded matches the JVM's
`ServiceLoader` ordering — typically classpath order. Multiple providers
are merged into a single `Properties` object, with later providers
overriding earlier ones.

## Quick rules of thumb

- **Static, namespace-wide value**: configmap.
- **Per-pipeline / per-connection override**: hint or JDBC property.
- **Tied to the JVM, not the data**: system property.
- **Computed at runtime from external state**: custom `ConfigProvider`.
- **Has a sensible default that's only overridden occasionally**: ship the
  default in the template (`{{key:default}}`), let users override via hint.

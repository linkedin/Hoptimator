# Config providers

A `ConfigProvider` is the SPI for "where do template placeholder values
come from?" The bundled providers read from `hoptimator-configmap` and
JVM system properties; you'd write your own when those aren't where your
configuration lives.

For the conceptual picture — three sources, precedence, file-like keys,
hint interaction — see [Kubernetes → Configuration](../kubernetes/templates.md).
This page covers just the SPI mechanics.

## When you'd write one

Common reasons to ship a custom `ConfigProvider`:

- **Centralized secrets store.** You'd rather Hoptimator pull credentials
  from Vault, AWS Secrets Manager, or an internal key store than from a
  ConfigMap.
- **Per-environment defaults from an existing config service.** Your org
  already has a config service that resolves environment + cluster +
  region into a properties bundle; reuse it instead of duplicating the
  state in a ConfigMap.
- **Dynamic values per connection.** You need values that depend on the
  caller — current user, originating fabric, request-scoped overrides.

If your need is "static, namespace-wide" the configmap is fine and you
don't need a custom provider.

## The interface

```java
public interface ConfigProvider {
  Properties loadConfig(Connection connection) throws Exception;
}
```

One method. Return a `Properties` populated however you like; throw if
loading fails — Hoptimator will log it and continue with whatever the
other providers produced.

The `Connection` argument is the active Hoptimator JDBC connection.
`HoptimatorConnection.connectionProperties()` and the bundled
`K8sContext.create(connection)` are useful when you want to scope your
loading by namespace, kubeconfig, or anything else the caller already
configured.

## Registering

Drop your class name in
`META-INF/services/com.linkedin.hoptimator.ConfigProvider`:

```
com.example.hoptimator.config.MyConfigProvider
```

All registered providers run on every connection and their outputs are
merged into one `Properties` object. Later providers override earlier
ones in the order `ServiceLoader` returns them — typically classpath
order.

## A sketch

```java
public class VaultConfigProvider implements ConfigProvider {

  @Override
  public Properties loadConfig(Connection connection) throws Exception {
    HoptimatorConnection conn = (HoptimatorConnection) connection;
    String namespace = K8sContext.create(conn).namespace();

    // Pull namespace-scoped values from Vault. Whatever you return becomes
    // available as {{key}} in any template — and as a connection property
    // anywhere ConfigService.config(...) is called.
    Properties p = new Properties();
    p.putAll(vaultClient.read("hoptimator/" + namespace));
    return p;
  }
}
```

Then in templates:

```yaml
yaml: |
  ...
  env:
    - name: API_KEY
      value: {{my-system.api-key}}
```

## Interaction with other sources

The order configurations are layered:

1. Every `ConfigProvider` runs and contributes a `Properties` set; later
   providers override earlier ones.
2. JDBC connection properties (URL params, `getConnection(props)`) and
   `Subscription.spec.hints` are merged on top — these always win.

So you can use a custom provider to set namespace-wide defaults and still
let users override them per-pipeline via hints, no extra code on your end.

## Caveats

- **Don't make `loadConfig` slow.** It runs on every Hoptimator connection
  and inside the operator's reconcile loop. Cache aggressively if your
  source has any meaningful latency.
- **Errors are tolerated.** If `loadConfig` throws, Hoptimator logs and
  continues with the other providers. That's the right default for
  "centralized service is down" scenarios but means you can't rely on a
  failed load to halt the operation — if a value is critical, validate
  for its presence in a `Validator` instead.
- **No service-loader ordering control.** If you need a specific
  precedence between providers, ensure your provider produces the
  authoritative key and let users override via hints (which always win)
  rather than relying on classpath ordering.

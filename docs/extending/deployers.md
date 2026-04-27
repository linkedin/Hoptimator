# Deployers

A `Deployer` knows how to materialize one type of pipeline element — a
`Source`, `Sink`, `Job`, `Trigger`, or anything else that implements
`Deployable`. The bundled deployers in `hoptimator-k8s` apply Kubernetes
manifests; the SPI is generic, so anything that knows how to "do something
with a spec" can plug in.

You'll need a deployer when:

- You're integrating a system whose resources are created via an admin API
  (REST, gRPC, command-line) rather than a CRD apply. Templates aren't
  enough — you need imperative code.
- You want pipelines to deploy to something other than Kubernetes — a
  Nomad cluster, an external service registry, your own internal control
  plane.

If your storage system has an operator that already accepts YAML, prefer a
`TableTemplate` ([Templates](../kubernetes/templates.md)) — the bundled
deployers handle the apply for you.

## The interface

```java
public interface Deployer {
  void create() throws SQLException;
  void update() throws SQLException;
  void delete() throws SQLException;

  /** Render a list of specs (usually YAML) without applying them. */
  List<String> specify() throws SQLException;

  /** Revert any changes made during the current operation. */
  void restore();
}
```

Five methods, three lifecycle expectations.

### `create` / `update` / `delete`

Idempotent operations. `create` should succeed if the resource already
exists in a compatible state; `update` should be safe to call repeatedly
with the same input; `delete` should succeed even if the resource is
missing. Don't assume "first time" — the operator's reconcile loop will
call these many times over the life of the resource.

### `specify`

Produce the spec that `create`/`update` *would* apply, without touching
anything. This is what backs the SQL CLI's `!specify` command and the MCP
`plan` tool. Keep it side-effect-free.

For Kubernetes-style deployers the typical implementation is "render the
template, return the rendered YAML." For systems that don't have a
declarative spec (REST APIs, etc.) it's reasonable to return a textual
description of the planned API calls, or even an empty list with a
comment — but `specify()` must not actually call out.

### `restore`

If `create` or `update` partially succeeds and then fails halfway through,
the deployer is asked to undo what it did. Track each side effect as you
make it; on `restore`, walk the list in reverse and undo each. The bundled
deployers keep an explicit list of rollback closures in a field; this is a
good pattern to copy.

## The provider

A `Deployer` doesn't get loaded directly. Instead, you ship a
`DeployerProvider`:

```java
public interface DeployerProvider {
  <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection);
  int priority();
}
```

For each `Deployable` (typically a `Source`, `Sink`, or `Job`), the
provider returns the deployers that apply to it. Return an empty
collection when the deployable isn't yours — the runtime will skip you and
move on to the next provider.

`priority()` controls ordering: providers with **lower priority numbers
run first**. If two providers can both deploy the same object, the lower-
priority one wins. The default Kubernetes provider is priority `2`; ship
`1` if you need to pre-empt it for specific cases, `3+` to fall back when
nothing else applies.

## Registering

Drop your provider's fully qualified class name in
`META-INF/services/com.linkedin.hoptimator.DeployerProvider`. One per
line; `#`-prefixed lines are ignored.

```
com.example.hoptimator.mysystem.MySystemDeployerProvider
```

## A concrete example: the Kafka deployer

The bundled Kafka path is a good shape to copy:

- [`KafkaDeployerProvider`](https://github.com/linkedin/Hoptimator/blob/main/hoptimator-kafka/src/main/java/com/linkedin/hoptimator/kafka/KafkaDeployerProvider.java)
  — type-checks the `Deployable`, extracts per-schema connection
  properties from the Calcite schema (i.e. the JDBC URL the `Database`
  CRD points at), and constructs the deployer.
- [`KafkaDeployer`](https://github.com/linkedin/Hoptimator/blob/main/hoptimator-kafka/src/main/java/com/linkedin/hoptimator/kafka/KafkaDeployer.java)
  — `create()` calls Kafka's AdminClient API to create the topic;
  `restore()` walks back and deletes any topic the current operation
  created; `specify()` returns the equivalent declarative spec.

The shape any provider should follow:

- Type-check the `Deployable` and return an empty collection if it's not
  what you handle.
- Extract any per-schema configuration from the connection.
- Construct one or more deployer instances and return them.

## Validation

Deployers can opt into pre-deploy validation by also implementing
`Validated`:

```java
public class MyDeployer implements Deployer, Validated {
  @Override
  public void validate(Validator.Issues issues) {
    // emit warnings or errors before any side effects
  }
}
```

`ValidationService.validateOrThrow(deployers)` is called before any
`create`/`update` happens — if any deployer reports `Issues.error(...)`,
the whole pipeline is rejected. See [Validators](validators.md) for more.

## Testing

The validate-then-specify path is the main test surface:

```java
Source source = new Source("my-database", List.of("MYSYS", "foo"), Map.of());
DeployerProvider provider = new MyDeployerProvider();
Collection<Deployer> deployers = provider.deployers(source, mockConnection);

assertThat(deployers).hasSize(1);
List<String> specs = deployers.iterator().next().specify();
assertThat(specs).contains("expected-yaml-fragment");
```

For end-to-end tests, the bundled `make integration-tests` target spins
up a real cluster and drives the deployers through `make
deploy-dev-environment`. Following that pattern is the safest way to
verify a new deployer doesn't break the operator's reconcile loop.

## Common pitfalls

- **Forgetting `restore()`**. If `create()` partially succeeds and then
  throws, and `restore()` is a no-op, the cluster is left in a partial
  state and the next reconcile may fail in confusing ways. Keep a list of
  rollback closures.
- **Side effects in `specify()`**. The dry-run path calls `specify()` and
  expects nothing to happen. If your `specify()` implementation needs to
  register temporary state (e.g. for the planner to resolve types), revert
  it in `restore()` so dry-runs don't leak.
- **Wrong priority**. If two providers both claim the same `Deployable`,
  only the lower-priority one runs. Ship priority `1` if you intentionally
  want to override the default Kubernetes path; `3+` if you want to
  fall back.
- **Catching too broad an exception**. The runtime distinguishes
  `SQLTransientException` (worth retrying) from `SQLNonTransientException`
  (terminal). Throw the right kind so the reconciler behaves correctly.

# Validators

A `Validator` inspects a SQL statement, a CRD, or a planned pipeline element
*before* anything is deployed and rejects it if it doesn't meet your
constraints. Validators are the right place for environment-specific policy:
naming conventions, schema compatibility checks, ACL enforcement, anything
where "this would deploy fine, but it shouldn't be allowed to."

## When validation runs

`ValidationService.validateOrThrow(...)` is called at three points in the
DDL path:

1. On the parsed SQL node (e.g. `SqlCreateMaterializedView`) — before any
   resolution or planning. Useful for syntax-level rules.
2. On the resolved `View` / `Source` / `Sink` after planning — the row type
   and connector configs are known at this point.
3. On the `Collection<Deployer>` produced by the deployer providers — last
   chance to reject before `create()` runs.

If any validator emits an `Issues.error(...)` at any of those points, the
whole operation aborts and the error message surfaces to the user (or to
the operator's status field for CRD-driven changes).

## The interfaces

```java
public interface Validated {
  void validate(Validator.Issues issues);
}

public interface Validator extends Validated {
  // Validators are themselves Validated — calling validate() on a validator
  // runs whatever check it encapsulates.
}

public interface ValidatorProvider {
  <T> Collection<Validator> validators(T obj);
}
```

Two ways to participate:

- **Make your own type `Validated`.** If you've authored a new
  `Deployable` (a custom `Source` subclass, a new kind of `Trigger`, etc.),
  implementing `Validated` directly is the simplest path.
- **Ship a `ValidatorProvider`.** A provider lets you attach validators to
  *any* type, including types you don't own (e.g. validating every
  `MaterializedView` regardless of who created it). This is the right
  mechanism for cross-cutting policy.

## The `Issues` API

Validators report findings into a tree-shaped `Issues` collector. The tree
is for organizing context — a single deploy can validate many objects, and
nested contexts let the final error message read like a structured report.

```java
public class NamingPolicyValidator implements Validator {
  @Override
  public void validate(Validator.Issues issues) {
    if (tableName.contains("_")) {
      issues.error("Table names must not contain underscores");
    }
    if (tableName.length() > 63) {
      issues.error("Table name exceeds 63 characters");
    }
    if (!tableName.matches("[a-z][a-z0-9-]*")) {
      issues.warn("Table name does not follow recommended naming conventions");
    }
  }
}
```

Three severity levels:

| Method            | Effect                                                                    |
| ----------------- | ------------------------------------------------------------------------- |
| `issues.info(s)`  | Surface information without affecting validity.                           |
| `issues.warn(s)`  | Warning; rendered in the report but does not fail validation.             |
| `issues.error(s)` | Error; the entire `Issues` tree is marked invalid and the operation fails. |

`Issues.child("context")` opens a sub-tree — useful when validating a
collection (e.g. one child per source in a pipeline).

There are a couple of helpers worth knowing about:

```java
// Reject any name containing uppercase, underscore, or period —
// the standard "Kubernetes subdomain" rule.
Validator.validateSubdomainName(name, issues);

// Reject duplicates within a collection, keyed by an accessor.
Validator.validateUnique(items, Item::name, issues);

// Stub for "I haven't implemented this yet but want to be loud about it."
Validator.notYetImplemented(issues);
```

## Registering a `ValidatorProvider`

Drop the fully qualified class name in
`META-INF/services/com.linkedin.hoptimator.ValidatorProvider`:

```
com.example.hoptimator.policy.MyPolicyValidatorProvider
```

A `ValidatorProvider` returns the validators that should run for a given
input:

```java
public class MyPolicyValidatorProvider implements ValidatorProvider {
  @Override
  public <T> Collection<Validator> validators(T obj) {
    if (obj instanceof Source) {
      return List.of(new NamingPolicyValidator((Source) obj));
    }
    if (obj instanceof MaterializedView) {
      return List.of(new SchemaCompatibilityValidator((MaterializedView) obj));
    }
    return List.of();
  }
}
```

Return an empty list when the object isn't yours. All providers run; their
outputs are aggregated into the same `Issues` tree.

## Built-in providers

The bundled providers are small enough to read end-to-end:

- `DefaultValidatorProvider` (`hoptimator-jdbc`) — calls `Validated`'s own
  `validate()` method when the object implements it. This is what makes
  `Validated`-on-your-own-type work without you having to ship a separate
  provider.
- `CompatibilityValidatorProvider` (`hoptimator-jdbc`) — runs cross-system
  compatibility checks, e.g. type conversion sanity between source and sink.
- `AvroValidatorProvider` (`hoptimator-avro`) — validates that the
  generated row types can be expressed in Avro, since most LinkedIn-style
  pipelines round-trip through Avro on the wire.

Reading them is the fastest way to see how a new provider should look.

## Patterns

### Reject early, reject loudly

Prefer making the rule fail at the parser stage (a validator on the
`SqlCreateMaterializedView` node) rather than at the deployer stage. The
error message will be clearer to the user and you avoid running the
planner unnecessarily.

### Keep validators side-effect-free

Validators run on `!specify` and the MCP `plan` tool, both of which are
expected to be safe dry-runs. Don't make API calls, don't write to
`Issues` from another thread, don't persist anything.

### Use `child(...)` for collections

When validating a list of sources, open one child context per source:

```java
for (Source source : pipeline.sources()) {
  validateSource(source, issues.child(source.table()));
}
```

The error report is much easier to read when each issue is grouped under
the source it pertains to.

### Don't break other people's pipelines

A `ValidatorProvider` runs on **every** matching object — it's
process-wide. If your validator returns errors for objects it doesn't
care about, you'll fail unrelated pipelines. Type-check (`obj instanceof
...`) defensively and return an empty collection for anything you don't
recognize.

## Testing

```java
Issues issues = new Issues("test");
new MyValidator(testSource).validate(issues);
issues.close();
assertThat(issues.valid()).isTrue();   // or assertThat(issues.toString()).contains("ERROR: ...")
```

The `Issues` object is `AutoCloseable`; closing it asserts that all child
contexts were also closed (i.e. nothing leaked). Using a try-with-resources
in tests is the safest pattern.

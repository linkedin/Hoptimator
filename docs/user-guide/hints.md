# Hints

Hints are key/value pairs you set at connection time that flow through the
planner into templates and connectors. Use them when you need to nudge a
specific deployment — pin Kafka partition counts, raise Flink parallelism,
inject a custom Kafka consumer property — without editing the underlying
templates.

Hints are **advisory**. If the planner picks a different physical pipeline
than you expected, hints that no longer apply are silently dropped.

## Where to set them

| Surface       | How                                                                                       |
| ------------- | ----------------------------------------------------------------------------------------- |
| SQL CLI       | `./hoptimator -u "jdbc:hoptimator://hints=key1=value1,key2=value2"`                       |
| JDBC          | `props.setProperty("hints", "key1=value1,key2=value2")`                                   |
| Subscription  | `spec.hints` map on a [`Subscription`](../getting-started/concepts.md#subscriptions) custom resource. |
| MCP           | Set via the JDBC URL the server is launched with. (No per-call override today.)           |

Format: comma-separated `KEY=VALUE` pairs. URL-encode values that contain
`,`, `=`, or `;` — both keys and values are URL-decoded after parsing.

## Two flavors

A hint's *key* determines what it does:

- **Template hints** (e.g. `kafka.partitions=4`, `flink.parallelism=2`)
  fill `{{kafka.partitions}}`-style placeholders in `TableTemplate` and
  `JobTemplate` YAML. The available keys are whatever the templates
  installed in your namespace reference — if the template doesn't have
  `{{kafka.partitions}}`, setting that hint is a no-op.
- **Connector hints** (e.g. `kafka.source.properties.group.id=my-group`)
  pass through to the engine, scoped by connector + direction. Format:
  `<connector>.<source|sink>.<config-name>`.

For the full template-authoring story — including the placeholder syntax,
matching rules, and the precedence between hints, configmap, and system
properties — see
[Templates and configuration](../kubernetes/templates.md).

## Planner hints

A few hint keys are interpreted by the planner itself rather than passed to a
template or connector:

- **`castMode`** controls how aggressively the planner reconciles a query
  column whose type differs from its sink column (for example a raw Kafka key
  exposed as `STRING` projected onto a typed `BIGINT` key column). See
  [Type checking and casts on write](ddl-reference.md#type-checking-and-casts-on-write)
  for the full behavior. Values:

  | Value              | Behavior                                                                       |
  | ------------------ | ------------------------------------------------------------------------------ |
  | `strict` (default) | Cast only assignment-compatible (same-family) types; otherwise fail early.     |
  | `assign`           | Also cast a character column onto a scalar sink column (e.g. `STRING → BIGINT`).|
  | `explicit`         | Also cast any explicitly-castable scalar pair — the deliberate "risky" opt-in. |

  A missing or unrecognized value falls back to `strict`, so a dropped hint can
  never silently widen the policy. A type mismatch that also crosses from a
  nullable source into a `NOT NULL` sink, and structural complex-type
  mismatches, remain errors at every level.

## Reading what was applied

After deployment, the `Pipeline` (and `Subscription`) `status` records the
hints that survived the plan. Useful when something didn't take effect:

```bash
kubectl get pipeline my-audience -o yaml | yq '.status.hints'
```

## Hint vs. template change

Rule of thumb:

- **One-off tuning for a single pipeline** → hint.
- **Every pipeline in a namespace should pick up the same value** → template
  with a default value.

Hints are the lowest-friction lever; they're also the easiest to lose track
of. For anything you'd want repeatable, prefer one of the durable options.

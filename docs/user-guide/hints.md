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
| Subscription  | `spec.hints` map on a [`Subscription`](../getting-started/concepts.md#subscriptions) CRD. |
| MCP           | Set via the JDBC URL the server is launched with. (No per-call override today.)           |

Format: comma-separated `KEY=VALUE` pairs. URL-encode values that contain
`,`, `=`, or `;` — both keys and values are URL-decoded after parsing.

## Two flavors

There are two distinct kinds of hint, distinguished only by their key.

### Template hints

Override `{{ }}` placeholders in `TableTemplate` and `JobTemplate` definitions.

For example, `kafka-template` includes `partitions: {{kafka.partitions:1}}`
and `parallelism: {{flink.parallelism:1}}`. To pin partitions to 4 and
parallelism to 2 for the next pipeline you create:

```bash
./hoptimator -u "jdbc:hoptimator://hints=kafka.partitions=4,flink.parallelism=2"
```

The keys you can set this way are determined by the templates installed in
your namespace — there's no fixed list. If the template doesn't reference
`{{ kafka.partitions }}`, setting that hint is a no-op.

### Connector hints

Pass configuration straight through to the engine, scoped by connector and
direction (source vs. sink).

Format: `<connector>.<source|sink>.<config-name>`.

Examples:

```bash
# Set the Kafka consumer group when Kafka is a source
./hoptimator -u "jdbc:hoptimator://hints=kafka.source.properties.group.id=my-group"

# Set Flink sink parallelism for Kafka sinks
./hoptimator -u "jdbc:hoptimator://hints=kafka.sink.sink.parallelism=2"

# Combine multiple
./hoptimator -u "jdbc:hoptimator://hints=kafka.source.properties.group.id=my-group,kafka.sink.sink.parallelism=2"
```

The middle segment selects which side the hint applies to:

| Segment   | Applies when the connector is the …          |
| --------- | -------------------------------------------- |
| `source`  | input side of a pipeline                     |
| `sink`    | output side of a pipeline                    |

Each engine's own connector docs are the source of truth for which configs
make sense — see, e.g., the
[Flink Kafka connector docs](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/kafka/).

## Reading what hints were applied

After deployment, the `Pipeline` (and `Subscription`) `status` records the
hints that survived the plan. Useful when something didn't take effect:

```bash
kubectl get pipeline my-audience -o yaml | yq '.status.hints'
```

## When to use a hint vs. a template change

Rule of thumb:

- **One-off tuning for a single pipeline** → hint.
- **Every pipeline in a namespace should pick up the same value** → put it in
  `hoptimator-configmap` and reference it from the template directly.
- **The template itself should change** → edit the `TableTemplate` /
  `JobTemplate` resource.

Hints are the lowest-friction lever; they're also the easiest to lose track
of. For anything you'd want repeatable, prefer one of the durable options.

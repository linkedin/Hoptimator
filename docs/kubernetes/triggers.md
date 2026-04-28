# Triggers

`TableTrigger` is Hoptimator's hook for running arbitrary work when an
upstream table changes — backfills, rETL refreshes, downstream
notifications, operational chores. The trigger spec is just a Kubernetes
Job (or CronJob) wrapped in a `TableTrigger` envelope, so you can do
anything Kubernetes can do.

This page is the operational guide. For the conceptual framing, see
[TableTriggers in concepts](../getting-started/index.md). For field-by-field
detail, see the [CRD reference](crd-reference.md#tabletrigger).

## Two firing modes

A trigger fires in one of two modes, picked by whether `spec.schedule` is
set:

| Mode               | When it fires                                                                                  |
| ------------------ | ---------------------------------------------------------------------------------------------- |
| **Cron-driven**    | `spec.schedule` is set. The operator fires the trigger on the cron cadence (`@hourly`, `0 */6 * * *`, etc.). |
| **Status-driven**  | `spec.schedule` is empty. The trigger fires whenever someone patches `status.timestamp`.      |

These can be combined — a trigger with both `schedule` set *and* receiving
status patches will fire on either condition.

### Cron-driven

The simplest case: refresh-every-N-minutes work that doesn't depend on
upstream events.

```yaml
apiVersion: hoptimator.linkedin.com/v1alpha1
kind: TableTrigger
metadata:
  name: refresh-audience
spec:
  schema: KAFKA
  table: audience
  schedule: "@hourly"
  yaml: |
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: refresh-audience-job
    spec:
      template:
        spec:
          containers:
            - name: refresh
              image: alpine/k8s:1.33.0
              command: ["bash", "-c", "echo refreshing $(date)"]
          restartPolicy: OnFailure
      backoffLimit: 4
      ttlSecondsAfterFinished: 90
```

Standard cron syntax applies. `@hourly`, `@daily`, etc. are accepted.

### Status-driven

The more interesting case: a producer of the upstream table fires the
trigger by patching its status. This pattern decouples producers from
consumers — the trigger is the contract, not the producer's code.

```bash
kubectl patch tabletrigger refresh-audience \
  -p "{\"status\":{\"timestamp\": \"$(date +%FT%TZ)\"}}" \
  --subresource=status --type=merge
```

That patch is exactly what the bundled `crontrigger` sample does on a
cron — simulating a producer that announces "data has arrived" once a
minute. See `deploy/samples/crontrigger.yaml`. The same approach works for
real producers: when a batch job finishes writing a partition, it patches
the corresponding `TableTrigger` and the consumer-side job runs.

The operator updates `status.watermark` to the timestamp of the most
recently *successfully* processed event, so consumers can tell which firings
were absorbed.

## Pause / resume

`spec.paused: true` stops the trigger from firing without deleting it.
Status patches and cron ticks are both ignored while paused.

```yaml
spec:
  paused: true
```

This is what `PAUSE TRIGGER` / `RESUME TRIGGER` flip in
[DDL](../user-guide/ddl-reference.md#create-trigger).

Use it when you want to stop running a trigger temporarily — e.g. during
an upstream backfill that would otherwise generate too many fires, or when
debugging a misbehaving job.

## `jobProperties`

`spec.jobProperties` is a free-form map of strings made available to the
templated job at runtime. Use it for runtime knobs that should be tied to
*this* trigger instance rather than baked into the YAML — connection
strings, partition names, source identifiers.

```yaml
spec:
  schema: BROOKLIN
  table: members
  jobProperties:
    source.dataset: members-v2
    source.partition: "2026-04-27"
  yaml: |
    apiVersion: batch/v1
    kind: Job
    spec:
      template:
        spec:
          containers:
            - name: backfill
              image: ...
              env:
                - name: SOURCE_DATASET
                  value: "{{source.dataset}}"
                - name: SOURCE_PARTITION
                  value: "{{source.partition}}"
```

The placeholders inside `yaml` are resolved against `jobProperties` (in
addition to the usual environment).

## Common patterns

### Offline-tier backfill

When a [LogicalTable](../getting-started/index.md) binds an offline tier,
Hoptimator auto-creates a `TableTrigger` named
`logical-<table>-offline-trigger`. Producers writing to the offline tier
patch the trigger's status; the trigger's job populates downstream
mirrors.

You don't author this trigger by hand — it falls out of the LogicalTable
declaration — but it's a useful pattern to recognize when reading the
deployed resources.

### rETL refresh

A status-driven trigger paired with a producer that knows when fresh
output is ready. The trigger's job is the rETL pipeline (Spark, MR,
Airflow-task). Producer pings the trigger; rETL runs.

### Downstream notification

Cron-driven trigger that calls an HTTP API, posts to Slack, or kicks off
a CI workflow whenever an upstream view changes. The trigger's job is a
small `curl` container.

### Operational hooks

Cron-driven trigger for periodic chores: cache invalidation, credential
rotation checks, ad-hoc validation queries. The trigger's job is whatever
container does the work.

## How to inspect

```bash
# Get the table-trigger view
kubectl get tabletriggers
# NAME              PAUSED   SCHEMA   TABLE   SCHEDULE   TIMESTAMP            WATERMARK
# refresh-audience  false    KAFKA    audience @hourly   2026-04-27T18:00:00Z 2026-04-27T18:00:01Z

# Inspect a single trigger and its launched jobs
kubectl describe tabletrigger refresh-audience

# See the spawned jobs (the operator tracks them in status.jobs)
kubectl get tabletrigger refresh-audience -o jsonpath='{.status.jobs}' | jq .
```

The Job/CronJob the trigger creates lives in the same namespace as the
trigger itself.

## When *not* to use a trigger

- **You want continuous data flow, not periodic firings.** That's a
  `Pipeline` — write a materialized view.
- **You want to react to row-level changes synchronously.** Triggers fire
  per-table, not per-row. For row-level reactions, the data plane (Flink
  job, etc.) is the right place.
- **You need cross-table consistency.** Triggers are independent — there's
  no "fire when *both* of these tables have advanced" primitive.

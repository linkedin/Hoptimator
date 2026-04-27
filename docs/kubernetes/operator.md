# The operator

`hoptimator-operator` is the long-running Kubernetes controller that
reconciles Hoptimator's CRDs. It uses the same Deployer machinery as the
SQL path — when a `Subscription`, `Pipeline`, `View`, or `TableTrigger`
changes, it asks the deployers to bring the cluster state in line with the
spec.

## What it watches

The operator runs three controllers, each with its own informer:

| Controller             | Watches          | What it does                                                                                            |
| ---------------------- | ---------------- | ------------------------------------------------------------------------------------------------------- |
| `PipelineReconciler`   | `Pipeline`       | Drives the deployers for each pipeline element to convergence. Updates `status.ready`, `failed`, `message`. |
| `TableTriggerReconciler` | `TableTrigger` | Fires the embedded job when the trigger's status timestamp is patched (or on the cron schedule). Tracks `status.timestamp` and `status.watermark`. |
| `ViewReconciler`       | `View`           | Reconciles standalone views.                                                                            |

`Subscription` and `LogicalTable` are typically reconciled at create time by
the SQL/JDBC path, but their `status` fields are populated by the same
deployer machinery so a subsequent edit is also handled correctly.

## Deploying

The operator ships as a Docker image (`docker.io/library/hoptimator` after
`make build`). Apply the manifests in `deploy/`:

```bash
kubectl apply -f deploy/serviceaccount.yaml
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/config/hoptimator-configmap.yaml
kubectl apply -f deploy/hoptimator-operator-deployment.yaml
```

The bundled `Makefile` wraps this in `make deploy` (and `make
deploy-demo`/`deploy-dev-environment` for the larger bundles that include
Flink, Kafka, etc.).

The deployment runs `PipelineOperatorApp.main` with a Calcite model URL
pointing at the configmap-mounted `model.yaml`:

```yaml
command:
  - "./hoptimator-operator-integration/bin/hoptimator-operator-integration"
  - "jdbc:calcite:model=/etc/config/model.yaml"
```

## Namespace scoping

By default, the operator watches **all namespaces** for the CRDs it owns.
To restrict it to one namespace, pass `--watch <namespace>`:

```yaml
command:
  - "./hoptimator-operator-integration/bin/hoptimator-operator-integration"
  - "jdbc:calcite:model=/etc/config/model.yaml"
  - "--watch=my-team"
```

The same effect can be achieved by setting `k8s.watch.namespace` as a
JDBC property in the model.

The operator's own pod can run anywhere with cluster-wide RBAC — the
namespace it lives in is independent of the namespaces it watches.

## RBAC

The minimum cluster role needed (from
[`deploy/rbac.yaml`](https://github.com/linkedin/Hoptimator/blob/main/deploy/rbac.yaml)):

```yaml
apiGroups: ["hoptimator.linkedin.com"]
resources:
  - acls
  - databases
  - engines
  - jobtemplates
  - kafkatopics
  - pipelines
  - sqljobs
  - subscriptions
  - tabletemplates
  - tabletriggers
  - views
verbs: ["get", "watch", "list", "update", "create"]
---
apiGroups: ["hoptimator.linkedin.com"]
resources:
  - acls/status
  - kafkatopics/status
  - pipelines/status
  - sqljobs/status
  - subscriptions/status
  - tabletriggers/status
verbs: ["get", "patch", "update", "create"]
---
apiGroups: ["flink.apache.org"]
resources: ["flinkdeployments", "flinksessionjobs"]
verbs: ["get", "update", "create"]
---
apiGroups: ["batch"]
resources: ["jobs"]
verbs: ["list", "create", "delete", "get"]
```

If you ship your own deployers that target other Kubernetes APIs, you'll
need to extend this role accordingly.

## Scaling and high availability

The default deployment runs **one replica**. The operator does not yet
support leader election, so running multiple replicas will result in
duplicate work — keep it at one for now. The reconciler is designed to
survive restarts: each loop reads the current spec and reconciles toward it,
so losing the pod is not destructive.

## Logs and debugging

```bash
kubectl logs -l app=hoptimator-operator -f
```

Useful things to grep:
- `Reconciling Pipeline` — controller picked up a change
- `Validating statement` — DDL validation runs through the same code path
- `Deployed` / `Failed to deploy` — terminal outcomes for an element
- `Restored` — a deployer rolled back after a failure mid-deploy

The operator does not yet emit Kubernetes events, so logs are the primary
debugging surface today.

## Lifecycle of a pipeline

1. Someone applies (or updates) a `Pipeline`, `Subscription`, or `View`.
2. The relevant reconciler picks up the change via the informer.
3. It builds the list of `Deployer` instances for the elements it owns —
   sources, sink, job, triggers — using the registered `DeployerProvider`s.
4. Each deployer's `create()` (or `update()`) is invoked.
5. On success, `status.ready=true`, `status.message="Ready."`.
6. On failure, all deployers `restore()` their changes and the controller
   surfaces the error in `status.failed`/`status.message`. The reconciler
   will retry on the next change event.

## When *not* to run the operator

The operator is only required when you want continuous reconciliation —
typically when applying CRDs via `kubectl` rather than driving everything
through the JDBC path. If your workflow is "developer runs `./hoptimator`
to create materialized views" and nothing else applies CRDs, you don't
strictly need the operator running. The CLI deploys synchronously and waits
for success.

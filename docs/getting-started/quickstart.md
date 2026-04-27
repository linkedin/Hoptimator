# Quickstart

This walkthrough takes about five minutes. By the end, you'll have Hoptimator
running against a local Kubernetes cluster, two demo databases registered, and
a materialized view turned into a real pipeline.

## Prerequisites

You need:

- **A local Kubernetes cluster.** The simplest option is Docker Desktop:
  install it, then enable Kubernetes under *Settings → Kubernetes*. Alternately
  use [kind](https://kind.sigs.k8s.io/).
- **`kubectl`** pointed at that cluster.
- **JDK 17+** and **Make**. The build uses the bundled Gradle wrapper.

Verify your context:

```bash
kubectl config current-context
# should print docker-desktop, or your kind cluster's name
```

## 1. Build and install the CLI

From the repo root:

```bash
make build install
```

This produces the `hoptimator` script (a wrapper around the SQL CLI) and the
container images used by the operator.

## 2. Deploy the demo

```bash
make deploy-demo
```

This installs the Hoptimator CRDs into your cluster and applies two demo
databases — `ads` and `profiles` — backed by an in-memory `demodb` source. It
also registers the table and job templates needed to deploy pipelines locally.

You should now see Hoptimator's resources in the cluster:

```bash
kubectl get databases
# NAME              SCHEMA    URL
# ads-database      ADS       jdbc:demodb://names=ads
# profile-database  PROFILE   jdbc:demodb://names=profile

kubectl get tabletemplates
kubectl get jobtemplates
```

## 3. Open the SQL CLI

```bash
./hoptimator
```

This launches a [sqlline](https://github.com/julianhyde/sqlline) shell connected
to `jdbc:hoptimator://`. Type `!intro` at the prompt for a tour. The most useful
commands to know up front:

| Command                          | What it does                                          |
| -------------------------------- | ----------------------------------------------------- |
| `!tables`                        | List every table in the catalog                       |
| `!schemas`                       | List every schema                                     |
| `!resolve ADS.PAGE_VIEWS`        | Show the schema and connector config for a table      |
| `!pipeline <sql>`                | Show the auto-generated pipeline SQL for a statement  |
| `!specify <sql>`                 | Show every Kubernetes spec that statement would emit  |
| `!quit`                          | Exit                                                  |

Try a quick query:

```sql
SELECT * FROM ADS.AD_CLICKS LIMIT 5;
```

## 4. Create a materialized view

Now do the interesting thing — declare a view that joins the two demo
databases:

```sql
CREATE MATERIALIZED VIEW ADS.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS
  NATURAL JOIN PROFILE.MEMBERS;
```

Hoptimator plans the join, picks the connectors, and asks the deployer to
materialize the resulting pipeline.

While you're iterating, use `CREATE OR REPLACE MATERIALIZED VIEW ...` to
update the same view in place. Without `OR REPLACE`, re-running a `CREATE`
for an existing name fails. Most of the development loop is faster if you
treat the view as mutable.

## 5. See what happened

In another terminal:

```bash
kubectl get views
# NAME          CATALOG   SCHEMA   VIEW       SQL
# ads-audience            ADS      AUDIENCE   SELECT FIRST_NAME, LAST_NAME ...

kubectl get pipelines
# NAME          SQL                                STATUS
# ads-audience   CREATE DATABASE IF NOT EXISTS `ADS` WITH ();...   Ready.
```

Each `Pipeline` is a complete, standalone Kubernetes resource. You can describe
it, watch its events, or `kubectl get -o yaml` it to see exactly what was
deployed.

If you want to see what Hoptimator *would have* deployed without actually
deploying it, the CLI has a dry-run mode for that:

```sql
!specify CREATE MATERIALIZED VIEW ADS.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS
```

## 6. Clean up

```sql
DROP MATERIALIZED VIEW ADS.AUDIENCE;
```

Or tear down the whole demo:

```bash
make undeploy-demo
```

## What's next

- Read [Concepts](concepts.md) to put names to what you just saw.
- Read the [Architecture overview](architecture.md) for the bigger picture of
  how SQL turns into running infrastructure.
- Try a richer setup with real Kafka, Flink, MySQL, and Venice clusters via
  `make deploy-dev-environment` (see the development guide when it lands).

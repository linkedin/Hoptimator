<div align="center">
  <h1>Hoptimator</h1>
  <h3>A Kubernetes-native control plane for multi-hop data pipelines</h3>
  <p>
    <a href="https://github.com/linkedin/Hoptimator/actions"><img src="https://img.shields.io/github/actions/workflow/status/linkedin/Hoptimator/integration-tests.yml?branch=main" alt="CI"></a>
    <a href="https://github.com/linkedin/Hoptimator/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-BSD--2--Clause-blue" alt="License"></a>
    <img src="https://img.shields.io/badge/status-alpha-orange" alt="Status: alpha">
  </p>
</div>

Hoptimator turns a single SQL statement into a running, multi-system data
pipeline. You declare what you want — a materialized view from one system into
another — and Hoptimator plans the topology, provisions the topics and jobs,
deploys them to Kubernetes, and reconciles them.

```sql
CREATE MATERIALIZED VIEW MY.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;
```

That one statement becomes:

- a `View` and a `Pipeline` Kubernetes resource,
- a connector configuration on each side,
- a Flink SQL job that maintains the result,
- and any intermediate hops (e.g. CDC topics) that the planner determined were
  necessary to get the data from sources to sink.

## Why Hoptimator?

- **One SQL surface across many systems.** Kafka, Flink, Venice, MySQL — and
  pluggable for the rest. The catalog is unified; joins span systems.
- **Multi-hop, declarative.** You don't write Flink jobs and you don't request
  topics. The planner figures out the topology from a query.
- **Kubernetes-native.** Pipelines are first-class CRDs. `kubectl apply`,
  `kubectl get`, `kubectl describe` — all work the way you'd expect.
- **Inspectable before it deploys.** `!specify` (CLI) and `plan` (MCP) emit the
  exact YAML Hoptimator would apply. No "magic" deploys.
- **Pluggable.** New sources, sinks, engines, deployers, and validators are all
  extension points on `hoptimator-api`.

## Quickstart

You need Docker Desktop with Kubernetes enabled (or `kind`), `kubectl`, and
JDK 17+. Then:

```bash
make build install     # build the project and install the SQL CLI
make deploy-demo       # install CRDs and a couple of demo databases
./hoptimator           # start the SQL CLI
> !intro
```

Inside the CLI, declare a materialized view:

```sql
CREATE MATERIALIZED VIEW MY.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;
```

Then in another terminal, watch what showed up:

```bash
kubectl get views
kubectl get pipelines
```

For a full walkthrough — including how to inspect the plan before deploying
and how to clean up — see the [Quickstart](docs/getting-started/quickstart.md).

## How it works

```
   SQL  ──▶  Planner  ──▶  Pipeline (sources, sink, job)
                              │
                              ▼
                          Deployers
                              │
                              ▼
                  Kubernetes resources
                  (Pipeline, KafkaTopic,
                   FlinkSessionJob, …)
                              │
                              ▼
                          Operator
                       (reconcile loop)
```

Hoptimator plays three roles: **planner** (parse + optimize the SQL across the
unified catalog), **adapter** (translate plan elements into target-system
specs), and **operator** (apply specs to Kubernetes and reconcile drift). The
same machinery powers the SQL CLI, the JDBC driver, the MCP server, and the
standalone operator.

For the long version, see the [Architecture overview](docs/getting-started/architecture.md).

## Documentation

The full docs live in [`docs/`](docs/index.md):

- **[Getting started](docs/getting-started/index.md)** — quickstart, concepts,
  architecture.
- **[User guide](docs/user-guide/index.md)** — SQL CLI, JDBC driver, MCP
  server, DDL reference, hints.
- **Kubernetes guide** *(coming soon)* — CRD reference, operator,
  configuration.
- **Extending Hoptimator** *(coming soon)* — writing deployers, connectors,
  validators, templates.
- **[Learn more](docs/resources/learn-more.md)** — engineering blog posts and
  case studies.

## Project status

Hoptimator is **alpha**. APIs — including the SQL grammar, the
`hoptimator-api` interfaces, and the `v1alpha1` CRDs — are subject to change
without notice. The project is still early-stage and experimental from an open
source perspective; if you adopt it today, expect to follow `main` and pin to
specific versions deliberately.

That said, Hoptimator is not a research toy: LinkedIn runs production
pipelines on it internally, including
[Apache Pinot ingestion](https://www.linkedin.com/blog/engineering/infrastructure/powering-apache-pinot-ingestion-with-hoptimator).
Pre-release artifacts for the modules in this repo are published to
[GitHub Packages](https://github.com/linkedin/Hoptimator/packages) and to
LinkedIn's [JFrog Artifactory](https://linkedin.jfrog.io/artifactory/hoptimator).

## Contributing

Bug reports, feature requests, and PRs are welcome. See
[CONTRIBUTING.md](CONTRIBUTING.md) for how to file an issue, send a pull
request, or report a security vulnerability.

## License

[BSD 2-Clause](LICENSE).

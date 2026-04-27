<div align="center">
  <h1>Hoptimator</h1>
  <h3>A SQL control plane for multi-system data pipelines</h3>
  <p>
    <a href="https://github.com/linkedin/Hoptimator/actions"><img src="https://img.shields.io/github/actions/workflow/status/linkedin/Hoptimator/integration-tests.yml?branch=main&label=CI" alt="CI"></a>
    <a href="https://github.com/linkedin/Hoptimator/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-BSD--2--Clause-blue" alt="License"></a>
    <img src="https://img.shields.io/badge/status-alpha-orange" alt="Status: alpha">
  </p>
</div>

Hoptimator turns SQL into running, multi-hop data pipelines that span
Kafka, Flink, Venice, and anything else you plug in. You declare what you
want — a materialized view from one system into another — and Hoptimator
plans the topology, generates the specs, deploys them, and reconciles them.

```sql
CREATE MATERIALIZED VIEW ADS.AUDIENCE AS
  SELECT FIRST_NAME, LAST_NAME
  FROM ADS.PAGE_VIEWS NATURAL JOIN PROFILE.MEMBERS;
```

What that statement *becomes* depends on the templates and databases
registered in your environment. With a typical Kafka + Flink setup, it
expands into:

- a `View` and a `Pipeline` resource,
- a connector configuration on each side,
- a Flink SQL job that maintains the result,
- and any intermediate hops (e.g. CDC topics) the planner determined were
  needed to get from sources to sink.

Swap in different templates and the same SQL can target a different stack.
The deployment target is pluggable — the bundled deployers target Kubernetes,
but `hoptimator-api` is the actual extension point.

## Why Hoptimator?

- **One SQL surface across many systems.** Kafka, Flink, Venice, MySQL — and
  pluggable for the rest. The catalog is unified; joins span systems.
- **Multi-hop, declarative.** You don't write Flink jobs and you don't request
  topics. The planner figures out the topology from a query.
- **Kubernetes out of the box, not as a hard requirement.** The bundled
  deployers target Kubernetes, so pipelines show up as first-class CRDs and
  `kubectl get pipelines` Just Works. The `Deployer` interface is the actual
  extension point — anything that knows how to materialize a spec can take
  the place of the defaults.
- **Inspectable before it deploys.** `!specify` (CLI) and `plan` (MCP) emit the
  exact specs Hoptimator would apply. No "magic" deploys.
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
CREATE MATERIALIZED VIEW ADS.AUDIENCE AS
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
- **[Kubernetes guide](docs/kubernetes/index.md)** — operator, CRD
  reference, templates, triggers, configuration.
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
pipelines on it internally. Pre-release artifacts for the modules in this
repo are published to LinkedIn's
[JFrog Artifactory](https://linkedin.jfrog.io/artifactory/hoptimator).

## Contributing

Bug reports, feature requests, and PRs are welcome. See
[CONTRIBUTING.md](CONTRIBUTING.md) for how to file an issue, send a pull
request, or report a security vulnerability.

## License

[BSD 2-Clause](LICENSE).

# Learn more

Selected external reading on Hoptimator's design and use at LinkedIn.

## Engineering blog posts

- [**Declarative data pipelines with Hoptimator**](https://www.linkedin.com/blog/engineering/data-streaming-processing/declarative-data-pipelines-with-hoptimator)
  — the original "what is this and why" post. Walks through the
  Subscription/materialized-view abstraction, the planner-as-orchestrator
  approach, and how Hoptimator unifies onboarding across systems like Kafka,
  Flink, Brooklin, Venice, and Espresso.
- [**Powering Apache Pinot ingestion with Hoptimator**](https://www.linkedin.com/blog/engineering/infrastructure/powering-apache-pinot-ingestion-with-hoptimator)
  — production case study. Describes how Pinot uses Hoptimator to manage its
  own ingestion pipelines: dynamic provisioning of Flink SQL jobs and Kafka
  topics, ML-driven partitioning, deduplication of overlapping pipeline
  components, and consumer-driven optimization.

## Where to follow along

- **GitHub Issues** — bug reports, feature requests, design discussions:
  <https://github.com/linkedin/Hoptimator/issues>
- **Releases** — version history and tags:
  <https://github.com/linkedin/Hoptimator/tags>

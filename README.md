# Hoptimator

Multi-hop declarative data pipelines

## Quick Start

```
  $ ./gradlew build
  $ ./bin/hoptimator
  > !intro
```

## What is Hoptimator?

Hoptimator is an SQL-based control plane for complex data pipelines.

Hoptimator turns high-level SQL _subscriptions_ into low-level SQL
_pipelines_. Pipelines may involve an auto-generated Flink job (or
similar) and any arbitrary resources required for the job to run. 

## How does it work?

Hoptimator has a pluggable _adapter_ framework, which lets you wire-up
arbtitary data sources. Adapters loosely correspond to "connectors"
in the underlying compute engine (e.g. Flink Connectors), but they may
bring along additional _baggage_. For example, an adapter may bring
along a cache or a CDC stream as part of the resulting pipeline.



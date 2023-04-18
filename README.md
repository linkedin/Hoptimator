# Hoptimator

Multi-hop declarative data pipelines

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

## Quick Start

```
  $ make quickstart
  ... wait a while ...
  $ ./bin/hoptimator
  > !intro
  > !q
```

## Subscriptions

Subscriptions are SQL views that are automatically materialized by a pipeline.

```
  $ kubectl apply -f deploy/samples/subscription.yaml
```

In response, the operator will deploy a Flink job and other resources:

```
  $ kubectl get subscriptions
  $ kubectl get sqljobs
  $ kubectl get flinkdeployments
  $ kubectl get kafkatopics
```

You can verify the job is running by inspecting the output:

```
  $ ./bin/hoptimator
  > !tables
  > SELECT * FROM RAWKAFKA."sample-subscription-1" LIMIT 5;
  > !q
```



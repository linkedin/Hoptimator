# Hoptimator

Multi-hop declarative data pipelines

## What is Hoptimator?

Hoptimator is an SQL-based control plane for complex data pipelines.

Hoptimator turns high-level SQL _subscriptions_ into multi-hop data pipelines. Pipelines may involve an auto-generated Flink job (or similar) and any arbitrary resources required for the job to run. 

## How does it work?

Hoptimator has a pluggable _adapter_ framework, which lets you wire-up arbtitary data sources. Adapters loosely correspond to connectors in the underlying compute engine (e.g. Flink Connectors), but they may include custom control plane logic. For example, an adapter may create a cache or a CDC stream as part of a pipeline. This enables a single pipeline to span multiple "hops" across different systems (as opposed to, say, a single Flink job).

Hoptimator's pipelines tend to have the following general shape:

                                    _________
    topic1 ----------------------> |         |
    table2 --> CDC ---> topic2 --> | SQL job | --> topic4
    table3 --> rETL --> topic3 --> |_________|


The three data sources on the left correspond to three different adapters:

1. `topic1` can be read directly from a Flink job, so the first adapter simply configures a Flink connector.
2. `table2` is inefficient for bulk access, so the second adapter creates a CDC stream (`topic2`) and configures a Flink connector to read from _that_.
3. `table3` is in cold storage, so the third adapter creates a reverse-ETL job to re-ingest the data into Kafka.

In order to deploy such a pipeline, you only need to write one SQL query, called a _subscription_. Pipelines are constructed automatically based on subscriptions.

## Quick Start

### Prerequistes

1. `docker` is installed and docker daemon is running
2. `kubectl` is installed and cluster is running
   1. `minikube` can be used for a local cluster
3. `helm` for Kubernetes is installed

### Run

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
  $ kubectl apply -f deploy/samples/subscriptions.yaml
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
  > SELECT * FROM RAWKAFKA."products" LIMIT 5;
  > !q
```

## The Operator

Hoptimator-operator is a Kubernetes operator that orchestrates multi-hop data pipelines based on Subscriptions (a custom resource). When a Subscription is deployed, the operator:

1. creates a _plan_ based on the Subscription SQL. The plan includes a set of _resources_ that make up a _pipeline_.
2. deploys each resource in the pipeline. This may involve creating Kafka topics, Flink jobs, etc.
3. reports Subscription status, which depends on the status of each resource in the pipeline.

The operator is extensible via _adapters_. Among other responsibilities, adapters can implement custom control plane logic (see `ControllerProvider`), or they can depend on external operators. For example, the Kafka adapter actively manages Kafka topics using a custom controller. The Flink adapter defers to [flink-kubernetes-operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/) to manage Flink jobs.

## The CLI

Hoptimator includes a SQL CLI based on [sqlline](https://github.com/julianhyde/sqlline). This is primarily for testing and debugging purposes, but it can also be useful for runnig ad-hoc queries. The CLI leverages the same adapters as the operator, but it doesn't deploy anything. Instead, queries run as local, in-process Flink jobs.

# Hoptimator

## Intro

Hoptimator gives you a SQL interface to a Kubernetes cluster. You can install databases, query tables, create views, and deploy data pipelines using just SQL.

To install a database, use `kubectl`:

```
 $ kubectl apply -f my-database.yaml
```

(`create database` is coming soon!)

Then use Hoptimator DDL to create a materialized view:

```
 > create materialized view my.foo as select * from ads.page_views;
```

Views created via DDL show up in Kubernetes as `views`:

```
 $ kubectl get views
 NAME     SCHEMA  VIEW  SQL
 my-foo   MY      FOO   SELECT *...

```

Materialized views result in `pipelines`:

```
 $ kubectl get pipelines
 NAME     SQL               STATUS
 my-foo   INSERT INTO...    Ready.
```

## Quickstart

Hoptimator requires a Kubernetes cluster. To connect from outside a Kubernetes cluster, make sure your `kubectl` is properly configured.

The below setup will install two local demo DBs, ads and profiles.

```
  $ make install            # build and install SQL CLI
  $ make deploy deploy-demo # install demo DB CRDs and K8s objects
  $ ./hoptimator            # start the SQL CLI
  > !intro
```

## Set up Kafka & Flink clusters

The below setup will install a Kafka and Flink cluster within Kubernetes.

```
  $ make install                                                    # build and install SQL CLI
  $ make deploy-dev-environment                                     # start all local dev setups
  $ kubectl port-forward -n kafka svc/one-kafka-external-0 9092 &   # forward external Kafka port for use by SQL CLI
  $ ./hoptimator                                                    # start the SQL CLI
  > !intro
```

## The SQL CLI

The `./hoptimator` script launches the [sqlline](https://github.com/julianhyde/sqlline) SQL CLI pre-configured to connect to `jdbc:hoptimator://`. The CLI includes some additional commands. See `!intro`.

## The JDBC Driver

To use Hoptimator from Java code, or from anything that supports JDBC, use the `jdbc:hoptimator://` JDBC driver.

## The Operator

`hoptimator-operator` turns materialized views into real data pipelines.

## Extending Hoptimator

Hoptimator can be extended via `TableTemplates`:

```
 $ kubectl apply -f my-table-template.yaml
```


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

## Set up dev environment

The below setup will create a dev environment with various resources within Kubernetes.

```
  $ make install                                                    # build and install SQL CLI
  $ make deploy-dev-environment                                     # start all local dev setups
  $ kubectl port-forward -n kafka svc/one-kafka-external-0 9092 &   # forward external Kafka port for use by SQL CLI
  $ ./hoptimator                                                    # start the SQL CLI
  > !intro
```

Commands `deploy-kafka`, `deploy-venice`, `deploy-flink`, etc. exist in isolation to deploy individual components.

### Kafka

To produce/consume Kafka data, use the following commands:

```
  $ kubectl run kafka-producer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server one-kafka-bootstrap.kafka.svc.cluster.local:9094 --topic existing-topic-1
  $ kubectl run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server one-kafka-bootstrap.kafka.svc.cluster.local:9094 --topic existing-topic-1 --from-beginning
```

### Flink

```
  $ kubectl get pods
  NAME                                              READY   STATUS    RESTARTS      AGE
  basic-session-deployment-7b94b98b6b-d6jt5         1/1     Running   0             43s
```

Once the Flink deployment pod has STATUS 'Running', you can forward port 8081 and connect to http://localhost:8081/
to access the Flink dashboard.

```
  $ kubectl port-forward basic-session-deployment-7b94b98b6b-d6jt5 8081 &
```

See the [Flink SQL Gateway Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/sql-gateway/overview/)
for sample adhoc queries through Flink.

To push a Flink job directly to the Flink deployment created above, `kubectl apply` the following yaml:
```
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkSessionJob
    metadata:
      name: test-flink-session-job
    spec:
      deploymentName: basic-session-deployment
      job:
        entryClass: com.linkedin.hoptimator.flink.runner.FlinkRunner
        args:
          - CREATE TABLE IF NOT EXISTS `datagen-table` (`KEY` VARCHAR, `VALUE` BINARY) WITH ('connector'='datagen', 'number-of-rows'='10');
          - CREATE TABLE IF NOT EXISTS `existing-topic-1` (`KEY` VARCHAR, `VALUE` BINARY) WITH ('connector'='kafka', 'properties.bootstrap.servers'='one-kafka-bootstrap.kafka.svc.cluster.local:9094', 'topic'='existing-topic-1', 'value.format'='json');
          - INSERT INTO `existing-topic-1` (`KEY`, `VALUE`) SELECT * FROM `datagen-table`;
        jarURI: file:///opt/hoptimator-flink-runner.jar
        parallelism: 1
        upgradeMode: stateless
        state: running
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


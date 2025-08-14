# Hoptimator

## Prerequisite
Hoptimator by default requires a Kubernetes cluster to operate. To set up a local dev environment, you can follow the steps below:

1. Install Docker for Desktop
2. Navigate to Docker Settings -> Kubernetes -> Select Enable Kubernetes
3. Run: kubectl config use-context docker-desktop

Alternatively you can use [kind](https://kind.sigs.k8s.io/) to create a local cluster.

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
  $ make build install      # build and install SQL CLI
  $ make deploy-demo        # install demo DB CRDs and K8s objects
  $ ./hoptimator            # start the SQL CLI
  > !intro
```

## Set up dev environment

The below setup will create a dev environment with various resources within Kubernetes.

```
  $ make build install                                              # build and install SQL CLI
  $ make deploy-dev-environment                                     # start all local dev setups
  $ kubectl port-forward -n kafka svc/one-kafka-external-bootstrap 9092 &   # forward external Kafka port for use by SQL CLI
  $ ./hoptimator                                                    # start the SQL CLI
  > !intro
```

Commands `deploy-kafka`, `deploy-venice`, `deploy-flink`, etc. exist in isolation to deploy individual components.

### Kafka

To produce/consume Kafka data, use the following commands:

```
  $ kubectl run kafka-producer -ti --image=quay.io/strimzi/kafka:0.46.0-kafka-4.0.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server one-kafka-bootstrap.kafka.svc.cluster.local:9094 --topic existing-topic-1
  $ kubectl run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.46.0-kafka-4.0.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server one-kafka-bootstrap.kafka.svc.cluster.local:9094 --topic existing-topic-1 --from-beginning
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
  $ kubectl port-forward svc/basic-session-deployment-rest 8081 &
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

The `./hoptimator` script launches the [sqlline](https://github.com/julianhyde/sqlline) SQL CLI pre-configured to connect to `jdbc:hoptimator://`.
The CLI includes some additional commands. See `!intro`.

## The JDBC Driver

To use Hoptimator from Java code, or from anything that supports JDBC, use the `jdbc:hoptimator://` JDBC driver.

## The MCP Server

To use Hoptimator from an AI chat bot, agent, IDE, etc, you can use the Model Context Protocol Server. Just point your MCP configs at the server path:

```
{
  "mcpServers": {
    "Hoptimator": {
      "command": "./start-mcp-server"
    }
}
```

You may need additional configuration, e.g. `JAVA_HOME`, depending on your environment.


## The Operator

`hoptimator-operator` turns materialized views into real data pipelines. The name operator comes from the Kubernetes Operator pattern.
`PipelineOperatorApp` is intended to be an entry point for a running application that can listen to and reconcile the resources created in Kubernetes by the K8s Deployers.
See [hoptimator-operator-deployment.yaml](deploy/hoptimator-operator-deployment.yaml) for K8s pod deployment of the operator.

## Extending Hoptimator

Hoptimator is extensible via `hoptimator-api`, which provides hooks for deploying, validating, and configuring the elements of a pipeline,
including external objects (e.g. Kafka topics) and data plane connectors (e.g. Flink connectors).
To deploy a source or sink, implement `Deployer<Source>`.
To deploy a job, implement `Deployer<Job>`.

In addition, the `k8s` catalog is itself highly extensible via `TableTemplates` and `JobTemplates`.
Generally, you can get Hoptimator to do what you want without writing any new code.

### Table Templates

`TableTemplates` let you specify how sources and sinks should be included in a pipeline. For example see [kafkadb.yaml](deploy/samples/kafkadb.yaml).

In this case, any tables within `kafka-database` will get deployed as `KafkaTopics` and use `kafka` connectors.

### Job Templates

`JobTemplates` are similar, but can embed SQL. For example see [flink-template.yaml](deploy/samples/flink-template.yaml).

In this case, any jobs created with this template will get deployed as `FlinkSessionJobs` within the `flink` namespace.

### Configuration

The `{{ }}` sections you see in the templates are variable placeholders that will be filled in by the Deployer.
See [Template.java](hoptimator-util/src/main/java/com/linkedin/hoptimator/util/Template.java) for how to specify templates.

While Deployers are extensible, today the primary deployer is to Kubernetes. These deployers 
[K8sSourceDeployer](hoptimator-k8s/src/main/java/com/linkedin/hoptimator/k8s/K8sSourceDeployer.java) (for table-templates)
and [K8sJobDeployer](hoptimator-k8s/src/main/java/com/linkedin/hoptimator/k8s/K8sJobDeployer.java) (for job-templates)
provide a few template defaults that you can choose to include in your templates:

K8sSourceDeployer: `name, database, schema, table, pipeline`

K8sJobDeployer: `name, database, schema, table, pipeline, sql, flinksql, flinkconfigs`

However, it is often a case where you want to add additional information to the templates that will be passed through during Source or Job creation.
There are two mechanisms to achieve this:

#### ConfigProvider

The ConfigProvider interface allows you to load additional configuration information that will be used during Source or Job creation.

The [K8sConfigProvider](hoptimator-k8s/src/main/java/com/linkedin/hoptimator/k8s/K8sConfigProvider.java) is the default one used in the K8s deployers.
K8sConfigProvider contains the ability to read configuration information via a Kubernetes configmap, `hoptimator-configmap`.
See [hoptimator-configmap.yaml](deploy/config/hoptimator-configmap.yaml) for information on how to use configmaps.

Configmaps are meant to be used for static configuration information applicable to the namespace `hoptimator-configmap` belongs to.

#### Hints

Users may want to provide additional information for their Job or Sink creation at runtime.
This can be done by adding hints as JDBC properties.

Hints are key-value pairs separated by an equals sign. Multiple hints are separated by a comma.

There are two ways to use hints.

1. Template hints can be used to override the `{{ }}` template specifications. 

For example, to specify the number of kafka partitions and the flink parallelism, you could add the following hints to the connection:
```
jdbc:hoptimator://hints=kafka.partitions=4,flink.parallelism=2
```
These fields can then be added to templates as `{{kafka.partitions}}` or `{{flink.parallelism}}` where applicable.

2. Connector hints allow the user to pass configurations directly through to an Engine (e.g. Flink).

Connector hints must be formatted as follows `<engine-connector-name>.<source|sink>.<configName>`

For example, to set a Kafka group id and startup mode to be used by Flink, you could add the following hints to the connection:
```
jdbc:hoptimator://hints=kafka.source.properties.group.id=4,kafka.sink.sink.parallelism=2
```
Field `properties.group.id` will be applied if the `kafka` connector is used by a source, and `sink.parallelism`
if the `kafka` connector is used by a sink.

Note that hints are simply recommendations, if the planner plans a different pipeline, they will be ignored.

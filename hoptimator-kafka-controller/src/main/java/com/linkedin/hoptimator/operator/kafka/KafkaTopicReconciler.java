package com.linkedin.hoptimator.operator.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;

import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicStatus;
import com.linkedin.hoptimator.operator.ConfigAssembler;
import com.linkedin.hoptimator.operator.Operator;


public class KafkaTopicReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(KafkaTopicReconciler.class);
  private final static String KAFKATOPIC = "hoptimator.linkedin.com/v1alpha1/KafkaTopic";

  private final Operator operator;

  public KafkaTopicReconciler(Operator operator) {
    this.operator = operator;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1KafkaTopic object = operator.<V1alpha1KafkaTopic>fetch(KAFKATOPIC, namespace, name);

      if (object == null) {
        log.info("Object {}/{} deleted. Skipping.", namespace, name);
        return new Result(false);
      }

      if (object.getStatus() == null) {
        object.setStatus(new V1alpha1KafkaTopicStatus());
      }

      String topicName = object.getSpec().getTopicName();
      Integer desiredPartitions = object.getSpec().getNumPartitions();
      Integer desiredReplicationFactor = object.getSpec().getReplicationFactor();

      // assemble AdminClient config
      ConfigAssembler assembler = new ConfigAssembler(operator);
      list(object.getSpec().getClientConfigs()).forEach(
          x -> assembler.addRef(namespace, x.getConfigMapRef().getName()));
      map(object.getSpec().getClientOverrides()).forEach((k, v) -> assembler.addOverride(k, v));
      Properties properties = assembler.assembleProperties();
      log.info("Using AdminClient config: {}", properties);

      AdminClient admin = AdminClient.create(properties);

      // Describe existing Kafka topic, if any
      try {
        log.info("Querying Kafka for topic {}...", topicName);
        TopicDescription topicDescription =
            admin.describeTopics(Collections.singleton(topicName)).all().get().get(topicName);

        log.info("Found existing topic {}", topicName);
        int actualPartitions = topicDescription.partitions().size();
        object.getStatus().setNumPartitions(actualPartitions);
        if (desiredPartitions != null && desiredPartitions > actualPartitions) {
          log.info("Desired partitions {} > actual partitions {}. Creating additional partitions.", desiredPartitions,
              actualPartitions);
          admin.createPartitions(Collections.singletonMap(topicName, NewPartitions.increaseTo(desiredPartitions)))
              .all()
              .get();
          object.getStatus().setNumPartitions(desiredPartitions);
        }
      } catch (ExecutionException e) {
        if (e.getCause() instanceof UnknownTopicOrPartitionException) {
          log.info("No existing topic {}. Will create it.", topicName);
          admin.createTopics(Collections.singleton(new NewTopic(topicName, Optional.ofNullable(desiredPartitions),
              Optional.ofNullable(desiredReplicationFactor).map(x -> x.shortValue())))).all().get();
          object.getStatus().setNumPartitions(desiredPartitions);
        } else {
          throw e;
        }
      } finally {
        admin.close();
      }

      operator.apiFor(KAFKATOPIC)
          .updateStatus(object, x -> object.getStatus())
          .onFailure(
              (x, y) -> log.error("Failed to update status of KafkaTopic {}/{}: {}.", namespace, name, y.getMessage()));
    } catch (Exception e) {
      log.error("Encountered exception while reconciling KafkaTopic {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    log.info("Done reconciling {}/{}", namespace, name);
    return new Result(false);
  }

  private static <T> List<T> list(List<T> maybeNull) {
    if (maybeNull == null) {
      return Collections.emptyList();
    } else {
      return maybeNull;
    }
  }

  private static <K, V> Map<K, V> map(Map<K, V> maybeNull) {
    if (maybeNull == null) {
      return Collections.emptyMap();
    } else {
      return maybeNull;
    }
  }
}


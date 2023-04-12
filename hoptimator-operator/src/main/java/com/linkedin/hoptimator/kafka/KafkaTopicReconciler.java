package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.Operator;
import com.linkedin.hoptimator.ConfigAssembler;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(KafkaTopicReconciler.class);

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
      V1alpha1KafkaTopic object = operator.<V1alpha1KafkaTopic>fetch("kafkatopic", namespace, name);
      String topicName = object.getSpec().getName();
      Integer desiredPartitions = object.getSpec().getNumPartitions();
      Integer desiredReplicationFactor = object.getSpec().getReplicationFactor();
      
      // assemble AdminClient config
      ConfigAssembler assembler = new ConfigAssembler(operator);
      list(object.getSpec().getClientConfigs()).forEach(x -> assembler.addRef(namespace, x.getConfigMapRef().getName()));
      map(object.getSpec().getClientOverrides()).forEach((k, v) -> assembler.addOverride(k, v));
      Properties properties = assembler.assembleProperties();
      log.info("Using AdminClient config: {}", properties);

      AdminClient admin = AdminClient.create(properties);

      // Describe existing Kafka topic, if any
      try {
        TopicDescription topicDescription = admin.describeTopics(Collections.singleton(topicName)).values().get(topicName).get();

        log.info("Found existing topic {}", topicName);
        int actualPartitions = topicDescription.partitions().size();
        if (desiredPartitions != null && desiredPartitions > actualPartitions) {
          log.info("Desired partitions {} > actual partitions {}. Creating additional partitions.",
            desiredPartitions, actualPartitions);
          admin.createPartitions(Collections.singletonMap(topicName, NewPartitions.increaseTo(desiredPartitions))).all().get();
        }
      } catch(ExecutionException e) {
        if (e.getCause() instanceof UnknownTopicOrPartitionException ) {
          log.info("No existing topic {}. Will create it.", topicName);
          admin.createTopics(Collections.singleton(new NewTopic(topicName, Optional.ofNullable(desiredPartitions),
            Optional.ofNullable(desiredReplicationFactor).map(x -> x.shortValue())))).all().get();
        } else {
          throw e;
        }
      }

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


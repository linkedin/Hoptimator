package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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


public class KafkaTopicReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(KafkaTopicReconciler.class);

  private final K8sContext context;
  private final K8sApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> kafkaTopicApi;

  public KafkaTopicReconciler(K8sContext context) {
    this(context, new K8sApi<>(context, KafkaControllerProvider.KAFKA_TOPICS));
  }

  KafkaTopicReconciler(K8sContext context, K8sApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> kafkaTopicApi) {
    this.context = context;
    this.kafkaTopicApi = kafkaTopicApi;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1KafkaTopic object;
      try {
        object = kafkaTopicApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {} deleted. Skipping.", name);
          return new Result(false);
        }
        throw e;
      }

      if (object.getStatus() == null) {
        object.setStatus(new V1alpha1KafkaTopicStatus());
      }

      String topicName = Objects.requireNonNull(object.getSpec()).getTopicName();
      Integer desiredPartitions = object.getSpec().getNumPartitions();
      Integer desiredReplicationFactor = object.getSpec().getReplicationFactor();

      // assemble AdminClient config
      ConfigAssembler assembler = new ConfigAssembler(context);
      list(object.getSpec().getClientConfigs()).forEach(
          x -> assembler.addRef(namespace, Objects.requireNonNull(x.getConfigMapRef()).getName()));
      map(object.getSpec().getClientOverrides()).forEach(assembler::addOverride);
      Properties properties = assembler.assembleProperties();
      log.info("Using AdminClient config: {}", properties);

      AdminClient admin = AdminClient.create(properties);

      // Describe existing Kafka topic, if any
      try {
        log.info("Querying Kafka for topic {}...", topicName);
        TopicDescription topicDescription =
            admin.describeTopics(Collections.singleton(topicName)).allTopicNames().get().get(topicName);

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
              Optional.ofNullable(desiredReplicationFactor).map(Integer::shortValue)))).all().get();
          object.getStatus().setNumPartitions(desiredPartitions);
        } else {
          throw e;
        }
      } finally {
        admin.close();
      }

      kafkaTopicApi.updateStatus(object, object.getStatus());
    } catch (InterruptedException | ExecutionException | SQLException e) {
      log.error("Encountered exception while reconciling KafkaTopic {}/{}", namespace, name, e);
      return new Result(true, failureRetryDuration());
    }
    log.info("Done reconciling {}/{}", namespace, name);
    return new Result(false);
  }

  private static <T> List<T> list(List<T> maybeNull) {
    return Objects.requireNonNullElse(maybeNull, Collections.emptyList());
  }

  private static <K, V> Map<K, V> map(Map<K, V> maybeNull) {
    return Objects.requireNonNullElse(maybeNull, Collections.emptyMap());
  }

  // TODO load from configuration
  protected Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  // TODO load from configuration
  protected Duration pendingRetryDuration() {
    return Duration.ofMinutes(1);
  }
}


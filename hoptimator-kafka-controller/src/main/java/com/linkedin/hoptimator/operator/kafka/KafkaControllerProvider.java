package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.k8s.K8sApiEndpoint;
import com.linkedin.hoptimator.k8s.K8sContext;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import com.linkedin.hoptimator.models.V1alpha1Acl;
import com.linkedin.hoptimator.models.V1alpha1AclList;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import com.linkedin.hoptimator.operator.ControllerProvider;


/** Provides a Controller plugin for KafkaTopics. */
public class KafkaControllerProvider implements ControllerProvider {

  public static final K8sApiEndpoint<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> KAFKA_TOPICS =
      new K8sApiEndpoint<>("KafkaTopic", "hoptimator.linkedin.com", "v1alpha1", "kafkatopics", false,
          V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class);
  public static final K8sApiEndpoint<V1alpha1Acl, V1alpha1AclList> ACLS =
      new K8sApiEndpoint<>("Acl", "hoptimator.linkedin.com", "v1alpha1", "acls", false,
          V1alpha1Acl.class, V1alpha1AclList.class);

  @Override
  public Collection<Controller> controllers(K8sContext context) {
    context.registerInformer(KAFKA_TOPICS, Duration.ofMinutes(5));
    // N.B. this shared CRD may be re-registered by other ControllerProviders
    context.registerInformer(ACLS, Duration.ofMinutes(5));

    Reconciler topicReconciler = new KafkaTopicReconciler(context);
    Controller topicController = ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(topicReconciler)
        .withName("kafka-topic-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaTopic.class, x).build())
        .build();

    Reconciler topicAclReconciler = new KafkaTopicAclReconciler(context);
    Controller topicAclController = ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(topicAclReconciler)
        .withName("kafka-topic-acl-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1Acl.class, x).build())
        .build();

    return Arrays.asList(topicController, topicAclController);
  }
}

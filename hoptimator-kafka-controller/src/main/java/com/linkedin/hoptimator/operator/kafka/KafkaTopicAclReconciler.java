package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.models.V1alpha1AclList;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;

import com.linkedin.hoptimator.models.V1alpha1Acl;
import com.linkedin.hoptimator.models.V1alpha1AclSpec;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.operator.ConfigAssembler;


public class KafkaTopicAclReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(KafkaTopicAclReconciler.class);

  private final K8sContext context;
  private final K8sApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> kafkaTopicApi;
  private final K8sApi<V1alpha1Acl, V1alpha1AclList> aclApi;

  public KafkaTopicAclReconciler(K8sContext context) {
    this(context, new K8sApi<>(context, KafkaControllerProvider.KAFKA_TOPICS),
        new K8sApi<>(context, KafkaControllerProvider.ACLS));
  }

  KafkaTopicAclReconciler(K8sContext context, K8sApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> kafkaTopicApi,
      K8sApi<V1alpha1Acl, V1alpha1AclList> aclApi) {
    this.context = context;
    this.kafkaTopicApi = kafkaTopicApi;
    this.aclApi = aclApi;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1Acl object;
      try {
        object = aclApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {} deleted. Skipping.", name);
          return new Result(false);
        }
        throw e;
      }

      String targetKind = Objects.requireNonNull(object.getSpec()).getResource().getKind();

      if (!"KafkaTopic".equals(targetKind)) {
        log.info("Not a KafkaTopic Acl. Skipping.");
        return new Result(false);
      }

      V1alpha1AclSpec.MethodEnum method = object.getSpec().getMethod();
      AclOperation operation;
      switch (method) {
        case READ:
          operation = AclOperation.READ;
          break;
        case WRITE:
          operation = AclOperation.WRITE;
          break;
        default:
          log.info("Unsupported KafkaTopic Acl operation {}. Skipping.", method);
          return new Result(false);
      }

      String targetName = object.getSpec().getResource().getName();
      String principal = object.getSpec().getPrincipal();

      V1alpha1KafkaTopic target = kafkaTopicApi.get(namespace, targetName);

      if (target == null) {
        log.info("Target KafkaTopic {}/{} not found. Retrying.", namespace, targetName);
        return new Result(true, failureRetryDuration());
      }

      // assemble AdminClient config
      ConfigAssembler assembler = new ConfigAssembler(context);
      list(Objects.requireNonNull(target.getSpec()).getClientConfigs()).forEach(
          x -> assembler.addRef(namespace, Objects.requireNonNull(x.getConfigMapRef()).getName()));
      map(target.getSpec().getClientOverrides()).forEach(assembler::addOverride);
      Properties properties = assembler.assembleProperties();
      log.info("Using AdminClient config: {}", properties);

      try (AdminClient admin = AdminClient.create(properties)) {
        log.info("Creating KafkaTopic Acl for {}...", target.getSpec().getTopicName());
        AclBinding binding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, target.getSpec().getTopicName(), PatternType.LITERAL),
            new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW));
        admin.createAcls(Collections.singleton(binding)).all().get();
        log.info("Granted {} {} access to {}.", principal, method, target.getSpec().getTopicName());
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling KafkaTopic Acl {}/{}", namespace, name, e);
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


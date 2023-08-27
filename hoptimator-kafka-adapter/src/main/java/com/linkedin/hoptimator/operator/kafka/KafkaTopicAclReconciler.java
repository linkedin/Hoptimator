package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.operator.ConfigAssembler;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1Acl;
import com.linkedin.hoptimator.models.V1alpha1AclSpec;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.clients.admin.AdminClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicAclReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(KafkaTopicAclReconciler.class);
  private final static String ACL = "hoptimator.linkedin.com/v1alpha1/Acl";
  private final static String KAFKATOPIC = "hoptimator.linkedin.com/v1alpha1/KafkaTopic";

  private final Operator operator;

  public KafkaTopicAclReconciler(Operator operator) {
    this.operator = operator;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1Acl object = operator.<V1alpha1Acl>fetch(ACL, namespace, name);

      if (object == null) {
        log.info("Object {}/{} deleted. Skipping.", namespace, name);
        return new Result(false);
      }

      String targetKind = object.getSpec().getResource().getKind();

      if (!targetKind.equals("KafkaTopic")) {
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

      V1alpha1KafkaTopic target = operator.<V1alpha1KafkaTopic>fetch(KAFKATOPIC, namespace, targetName);

      if (target == null) {
        log.info("Target KafkaTopic {}/{} not found. Retrying.", namespace, targetName);
        return new Result(true, operator.failureRetryDuration());
      }

      // assemble AdminClient config
      ConfigAssembler assembler = new ConfigAssembler(operator);
      list(target.getSpec().getClientConfigs()).forEach(x ->
        assembler.addRef(namespace, x.getConfigMapRef().getName()));
      map(target.getSpec().getClientOverrides()).forEach((k, v) -> assembler.addOverride(k, v));
      Properties properties = assembler.assembleProperties();
      log.info("Using AdminClient config: {}", properties);

      AdminClient admin = AdminClient.create(properties);
      try {
        log.info("Creating KafkaTopic Acl for {}...", target.getSpec().getTopicName());
        AclBinding binding = new AclBinding(new ResourcePattern(ResourceType.TOPIC,
          target.getSpec().getTopicName(), PatternType.LITERAL), new AccessControlEntry(
          principal, "*", operation, AclPermissionType.ALLOW));
        admin.createAcls(Collections.singleton(binding)).all().get();
        log.info("Granted {} {} access to {}.", principal, method, target.getSpec().getTopicName());
      } finally {
        admin.close();
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling KafkaTopic Acl {}/{}", namespace, name, e);
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


package com.linkedin.hoptimator.k8s.status;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;

/**
 * Estimates or guesses the status of an element of a {@link com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline} by inspecting its internal state.
 */
public class K8sPipelineElementStatusEstimator {
  private final static Logger log = LoggerFactory.getLogger(K8sPipelineElementStatusEstimator.class);

  private final K8sContext context;

  public K8sPipelineElementStatusEstimator(K8sContext context) {
    this.context = context;
  }

  /**
   * Returns statuses of all elements specified in the given pipeline.
   */
  public List<K8sPipelineElementStatus> estimateStatuses(V1alpha1Pipeline pipeline) {
    String namespace = pipeline.getMetadata().getNamespace();
    return Arrays.stream(pipeline.getSpec().getYaml().split("\n---\n"))
        .map(String::trim)
        .filter(x -> !x.isEmpty())
        .map(yaml -> estimateElementStatus(yaml, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Estimates status of an element. If we can not retrieve it from K8s, we assume that it's not ready and not failed yet.
   */
  private K8sPipelineElementStatus estimateElementStatus(String elementYaml, String pipelineNamespace) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(elementYaml);
    String name = obj.getMetadata().getName();
    String namespace = obj.getMetadata().getNamespace() == null ? pipelineNamespace : obj.getMetadata().getNamespace();
    String kind = obj.getKind();
    String nameWithKind = String.format("%s/%s", kind, name);
    try {
      KubernetesApiResponse<DynamicKubernetesObject> existing =
          context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).get(namespace, name);
      String failureMessage =
          String.format("Failed to fetch %s in namespace %s: %s.", nameWithKind, namespace, existing.toString());
      existing.onFailure((code, status) -> log.warn(failureMessage));
      if (!existing.isSuccess()) {
        return defaultUnreadyStatusOnK8sObjectRetrievalFailure(nameWithKind, failureMessage);
      }
      K8sPipelineElementStatus elementStatus = estimateDynamicObjectStatus(nameWithKind, existing.getObject());
      if (elementStatus.isReady()) {
        log.info("{} is ready.", nameWithKind);
      } else {
        log.info("{} is NOT ready.", nameWithKind);
      }
      return elementStatus;
    } catch (Exception e) {
      String failureMessage =
          String.format("Encountered exception while checking status of %s in namespace %s: %s", nameWithKind,
              namespace, e);
      log.error(failureMessage);
      return defaultUnreadyStatusOnK8sObjectRetrievalFailure(nameWithKind, failureMessage);
    }
  }

  public static K8sPipelineElementStatus estimateDynamicObjectStatus(String name, DynamicKubernetesObject obj) {
    // We make a best effort to guess the status of the dynamic object. By default, it's ready.
    if (obj == null || obj.getRaw() == null) {
      return defaultUnreadyStatusOnK8sObjectRetrievalFailure(name, "Returned K8s object is null or has no json");
    }

    K8sPipelineElementStatus status = estimateBasedOnTopLevelStatusField(name, obj);
    if (status != null) {
      return status;
    }

    // TODO: Look for common Conditions
    String message =
        String.format("Object %s/%s/%s considered ready by default.", obj.getMetadata().getNamespace(), obj.getKind(),
            obj.getMetadata().getName());
    log.warn(message);
    return new K8sPipelineElementStatus(name, true, false, message);
  }

  private static K8sPipelineElementStatus estimateBasedOnTopLevelStatusField(String name, DynamicKubernetesObject obj) {
    if (!obj.getRaw().has("status")) {
      return null;
    }

    JsonObject statusJson = obj.getRaw().get("status").getAsJsonObject();
    K8sPipelineElementStatus elementStatus;
    elementStatus = estimateBasedOnStatusReadyField(name, statusJson);
    if (elementStatus == null) {
      elementStatus = estimateBasedOnStatusStateField(name, statusJson);
    }
    if (elementStatus == null) {
      elementStatus = estimateBasedOnJobStatusStateField(name, statusJson);
    }

    return elementStatus;
  }

  private static K8sPipelineElementStatus estimateBasedOnStatusReadyField(String elementName, JsonObject statusJson) {
    try {
      boolean ready = statusJson.get("ready").getAsBoolean();
      boolean failed = statusJson.has("failed") && statusJson.get("failed").getAsBoolean();
      String message = statusJson.has("message") ? statusJson.get("message").getAsString() : "";
      return new K8sPipelineElementStatus(elementName, ready, failed, message);
    } catch (Exception e) {
      log.debug("Exception looking for .status.ready. Swallowing.", e);
    }
    return null;
  }

  private static K8sPipelineElementStatus estimateBasedOnStatusStateField(String elementName, JsonObject statusJson) {
    try {
      String statusState = statusJson.get("state").getAsString();
      return fromStateString(elementName, statusState);
    } catch (Exception e) {
      log.debug("Exception looking for .status.state. Swallowing.", e);
    }
    return null;
  }

  private static K8sPipelineElementStatus fromStateString(String elementName, String state) {
    boolean ready = state.matches("(?i)READY|RUNNING|FINISHED");
    boolean failed = state.matches("(?i)CRASHLOOPBACKOFF|FAILED");
    return new K8sPipelineElementStatus(elementName, ready, failed, state);
  }

  private static K8sPipelineElementStatus estimateBasedOnJobStatusStateField(String elementName,
      JsonObject statusJson) {
    try {
      String jobState = statusJson.get("jobStatus").getAsJsonObject().get("state").getAsString();
      return fromStateString(elementName, jobState);
    } catch (Exception e) {
      log.debug("Exception looking for .status.jobStatus.state. Swallowing.", e);
    }
    return null;
  }

  /**
   * Defaults to unready state when we cannot retrieve the object from K8s.
   */
  private static K8sPipelineElementStatus defaultUnreadyStatusOnK8sObjectRetrievalFailure(String elementName,
      String errorMessage) {
    return new K8sPipelineElementStatus(elementName, false, false, errorMessage);
  }
}

package com.linkedin.hoptimator.k8s;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatusEstimator;
import com.linkedin.hoptimator.util.Api;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Provides all pipeline elements in a {@link com.linkedin.hoptimator.k8s.K8sContext} instance. */
public class K8sPipelineElementApi implements Api<K8sPipelineElement> {
  private final static Logger log = LoggerFactory.getLogger(K8sPipelineElementApi.class);
  private final K8sContext context;

  K8sPipelineElementApi(K8sContext context) {
    this.context = context;
  }

  private Collection<K8sPipelineElement> discoverAllElements(K8sContext context) throws SQLException {
    final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi = new K8sApi<>(context, K8sApiEndpoints.PIPELINES);
    Collection<V1alpha1Pipeline> pipelines = pipelineApi.list();

    Map<String, K8sPipelineElement> elements = new HashMap<>();
    K8sPipelineElementStatusEstimator statusEstimator = new K8sPipelineElementStatusEstimator(context);
    for (V1alpha1Pipeline pipeline : pipelines) {
      String namespace = Objects.requireNonNull(pipeline.getMetadata()).getNamespace();
      List<String> elementYamls = getPipelineElements(pipeline);
      for (String elementYaml : elementYamls) {
        K8sPipelineElementStatus elementStatus = statusEstimator.estimateElementStatus(elementYaml, namespace);
        Map<String, String> configurations = getElementConfiguration(elementYaml, namespace);
        String key = elementStatus.getName();
        if (!elements.containsKey(key)) {
          elements.put(key, new K8sPipelineElement(pipeline, elementStatus, configurations));
        }
        elements.get(key).addPipeline(pipeline);
      }
    }
    return elements.values();
  }

  /**
   * Returns list of all elements specified in the given pipeline.
   */
  List<String> getPipelineElements(V1alpha1Pipeline pipeline) {
    return Arrays.stream(Objects.requireNonNull(Objects.requireNonNull(pipeline.getSpec()).getYaml())
            .split("\n---\n"))
        .map(String::trim)
        .filter(x -> !x.isEmpty())
        .collect(Collectors.toList());
  }

  /**
   * Returns spec.configs of the given element, if present.
   */
  Map<String, String> getElementConfiguration(String elementYaml, String pipelineNamespace) {
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
        return new HashMap<>();
      }
      DynamicKubernetesObject element = existing.getObject();
      if (element == null || element.getRaw() == null) {
        return new HashMap<>();
      }
      if (!element.getRaw().has("spec") || !element.getRaw().getAsJsonObject("spec").has("configs")) {
        return new HashMap<>();
      }

      // Extract configs as a Map
      JsonObject configs = element.getRaw().getAsJsonObject("spec").getAsJsonObject("configs");
      Map<String, String> configMap = new HashMap<>();
      for (Map.Entry<String, JsonElement> entry : configs.entrySet()) {
        configMap.put(entry.getKey(), entry.getValue().getAsString());
      }
      return configMap;
    } catch (Exception e) {
      String failureMessage =
          String.format("Encountered exception while checking status of %s in namespace %s: %s", nameWithKind,
              namespace, e);
      log.error(failureMessage);
      return new HashMap<>();
    }
  }

  /**
   * Lists all pipeline elements in the context.
   */
  @Override
  public Collection<K8sPipelineElement> list() throws SQLException {
    return discoverAllElements(context);
  }
}

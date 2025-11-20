package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;


public class FakeK8sYamlApi extends K8sYamlApi {

  private final Map<String, String> nameToYaml;

  public FakeK8sYamlApi(Map<String, String> yamls) {
    super(null);
    this.nameToYaml = yamls;
  }

  @Override
  public DynamicKubernetesObject getIfExists(DynamicKubernetesObject obj) throws SQLException {
    String name = obj.getMetadata().getName();
    return nameToYaml.containsKey(name) ? Dynamics.newFromYaml(nameToYaml.get(name)) : null;
  }

  @Override
  public void createWithMetadata(DynamicKubernetesObject obj, Map<String, String> annotations,
      Map<String, String> labels, List<V1OwnerReference> ownerReferences) throws SQLException {
    if (annotations != null) {
      if (obj.getMetadata().getAnnotations() == null) {
        obj.getMetadata().setAnnotations(annotations);
      } else {
        obj.getMetadata().getAnnotations().putAll(annotations);
      }
    }

    if (labels != null) {
      if (obj.getMetadata().getLabels() == null) {
        obj.getMetadata().setLabels(labels);
      } else {
        obj.getMetadata().getLabels().putAll(labels);
      }
    }

    String dump = Yaml.dump(obj);
    System.out.println("Created:");
    System.out.println(dump);
    System.out.println();
    nameToYaml.put(obj.getMetadata().getName(), dump);
  }

  @Override
  public void delete(String apiVersion, String kind, String namespace, String name) throws SQLException {
    System.out.println("Deleted:");
    System.out.printf("%s:%s:%s:%s%n", apiVersion, kind, namespace, name);
    System.out.println();
    nameToYaml.remove(name);
  }

  @Override
  public void update(DynamicKubernetesObject obj) throws SQLException {
    nameToYaml.put(obj.getMetadata().getName(), Yaml.dump(obj));
  }
}

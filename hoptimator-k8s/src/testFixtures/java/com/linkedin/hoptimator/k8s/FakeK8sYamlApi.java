package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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
  public DynamicKubernetesObject get(String yaml, boolean throwIfNotExists) throws SQLException {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String name = obj.getMetadata().getName();

    if (throwIfNotExists && !nameToYaml.containsKey(name)) {
      throw new SQLException("Object not found: " + name);
    }
    return nameToYaml.containsKey(name) ? Dynamics.newFromYaml(nameToYaml.get(name)) : null;
  }

  @Override
  public void create(String yaml) throws SQLException {
    createWithAnnotationsAndLabels(yaml, new HashMap<>(), new HashMap<>());
  }

  @Override
  public void createWithAnnotationsAndLabels(String yaml, Map<String, String> annotations,
      Map<String, String> labels) throws SQLException {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    if (obj.getMetadata().getAnnotations() == null) {
      obj.getMetadata().setAnnotations(annotations);
    } else {
      obj.getMetadata().getAnnotations().putAll(annotations);
    }

    if (obj.getMetadata().getLabels() == null) {
      obj.getMetadata().setLabels(labels);
    } else {
      obj.getMetadata().getLabels().putAll(labels);
    }

    String dump = Yaml.dump(obj);
    System.out.println("Created:");
    System.out.println(dump);
    System.out.println();
    nameToYaml.put(obj.getMetadata().getName(), dump);
  }

  @Override
  public void delete(String yaml) throws SQLException {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    System.out.println("Deleted:");
    System.out.println(yaml);
    System.out.println();
    nameToYaml.remove(obj.getMetadata().getName());
  }

  @Override
  public void update(String yaml) throws SQLException {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    update(obj);
  }

  @Override
  public void update(DynamicKubernetesObject obj) throws SQLException {
    nameToYaml.put(obj.getMetadata().getName(), Yaml.dump(obj));
  }
}

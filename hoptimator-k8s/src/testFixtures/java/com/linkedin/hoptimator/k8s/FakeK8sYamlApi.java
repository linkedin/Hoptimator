package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;


public class FakeK8sYamlApi extends K8sYamlApi {

  private final List<String> yamls;

  public FakeK8sYamlApi(List<String> yamls) {
    super(null);
    this.yamls = yamls;
  }

  @Override
  public void create(String yaml) throws SQLException {
    createWithAnnotationsAndLabels(yaml, null, null);
  }

  @Override
  public void createWithAnnotationsAndLabels(String yaml, Map<String, String> annotations,
      Map<String, String> labels) throws SQLException {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    obj.setMetadata(obj.getMetadata().annotations(annotations).labels(labels));
    String dump = Yaml.dump(obj);
    System.out.println("Created:");
    System.out.println(dump);
    System.out.println();
    yamls.add(dump);
  }

  @Override
  public void delete(String yaml) throws SQLException {
    System.out.println("Deleted:");
    System.out.println(yaml);
    System.out.println();
    yamls.remove(yaml);
  }

  @Override
  public void update(String yaml) throws SQLException {
    yamls.remove(yaml);
    yamls.add(yaml);
  }
}

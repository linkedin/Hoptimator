package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Deployer;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.representer.Representer;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;


public abstract class K8sDeployer<T extends KubernetesObject, U extends KubernetesListObject>
    implements Deployer {

  private final K8sApi<T, U> api;
  private final K8sSnapshot snapshot;

  protected K8sDeployer(K8sContext context, K8sApiEndpoint<T, U> endpoint) {
    this.api = createApi(context, endpoint);
    this.snapshot = createSnapshot(context);
  }

  K8sApi<T, U> createApi(K8sContext context, K8sApiEndpoint<T, U> endpoint) {
    return new K8sApi<>(context, endpoint);
  }

  K8sSnapshot createSnapshot(K8sContext context) {
    return new K8sSnapshot(context);
  }

  @Override
  public void create() throws SQLException {
    create(toK8sObject());
  }

  public V1OwnerReference createAndReference() throws SQLException {
    T obj = toK8sObject();
    create(obj);
    return api.reference(obj);
  }

  private void create(T obj) throws SQLException {
    snapshot.store(obj);
    api.create(obj);
  }

  @Override
  public void delete() throws SQLException {
    T obj = toK8sObject();
    snapshot.store(obj);
    api.delete(obj);
  }

  @Override
  public void update() throws SQLException {
    update(toK8sObject());
  }

  public V1OwnerReference updateAndReference() throws SQLException {
    T obj = toK8sObject();
    update(obj);
    return api.reference(obj);
  }

  private void update(T obj) throws SQLException {
    snapshot.store(obj);
    api.update(obj);
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.singletonList(dumpYaml(toK8sObject()));
  }

  // SnakeYAML's default RepresentEnum uses Enum.name(), which returns the Java constant name
  // (e.g. "MYSQL") rather than the CRD-defined string value (e.g. "MySQL"). Override the enum
  // representer to use toString() instead, which these generated enums override to return value.
  private static String dumpYaml(Object obj) {
    DumperOptions opts = new DumperOptions();
    opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    Representer representer = new Representer(opts) {
      {
        multiRepresenters.put(Enum.class, data -> representScalar(Tag.STR, data.toString()));
      }

      @Override
      protected NodeTuple representJavaBeanProperty(Object javaBean, Property property,
          Object propertyValue, Tag customTag) {
        if (propertyValue == null) {
          return null;
        }
        return super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
      }

      @Override
      protected MappingNode representJavaBean(Set<Property> properties, Object javaBean) {
        MappingNode node = super.representJavaBean(properties, javaBean);
        node.setTag(Tag.MAP);
        node.getValue().sort(Comparator.comparing(t -> ((ScalarNode) t.getKeyNode()).getValue()));
        return node;
      }
    };
    return new org.yaml.snakeyaml.Yaml(representer, opts).dump(obj);
  }

  @Override
  public void restore() {
    snapshot.restore();
  }


  protected abstract T toK8sObject() throws SQLException;
}

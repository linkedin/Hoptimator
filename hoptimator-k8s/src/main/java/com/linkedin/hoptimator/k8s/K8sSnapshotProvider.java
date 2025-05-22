package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.SnapshotProvider;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// Grabs information from each K8s spec to snapshot the current state of the resource.
// On restore, the last state of each spec will be reapplied.
// Handles deletion if the resource previously did not exist.
public class K8sSnapshotProvider implements SnapshotProvider {
  private static final Logger log = LoggerFactory.getLogger(K8sSnapshotProvider.class);

  // Mapping of K8sSpec to existing object
  private final Map<K8sSpec, DynamicKubernetesObject> newToOldMap = new HashMap<>();

  private K8sYamlApi api;

  // Needed for ServiceLoader
  public K8sSnapshotProvider() {
  }

  // Used for testing purposes
  K8sSnapshotProvider(K8sYamlApi api) {
    this.api = api;
  }

  @Override
  public <T> void store(T obj, Properties connectionProperties) throws SQLException {
    // Must support the more generic KubernetesObject and not DynamicKubernetesObject as generated types
    // such as V1alpha1Pipeline implement KubernetesObject
    if (!(obj instanceof KubernetesObject)) {
      return;
    }
    DynamicKubernetesObject spec = toDynamicKubernetesObject((KubernetesObject) obj);

    if (api == null) {
      this.api = new K8sYamlApi(K8sContext.create(connectionProperties));
    }
    spec = this.api.setNamespaceFromContext(spec);

    K8sSpec k8sSpec = new K8sSpec(spec);
    if (newToOldMap.containsKey(k8sSpec)) {
      // Nothing to store, want to keep the oldest version of a spec
      return;
    }
    DynamicKubernetesObject existing = api.getIfExists(spec);
    newToOldMap.put(k8sSpec, existing);
    log.info("Successfully snapshot K8s obj: {}", spec);
  }

  public static DynamicKubernetesObject toDynamicKubernetesObject(KubernetesObject obj) {
    if (obj instanceof DynamicKubernetesObject) {
      return (DynamicKubernetesObject) obj;
    }
    DynamicKubernetesObject dynamicObject = new DynamicKubernetesObject();
    dynamicObject.setApiVersion(obj.getApiVersion());
    dynamicObject.setKind(obj.getKind());
    dynamicObject.setMetadata(obj.getMetadata());
    return dynamicObject;
  }

  @Override
  public void restore() {
    if (api == null) {
      log.warn("K8sSnapshotProvider not initialized. Skipping restore operation.");
      return;
    }
    for (Map.Entry<K8sSpec, DynamicKubernetesObject> entry : newToOldMap.entrySet()) {
      try {
        K8sSpec spec = entry.getKey();
        DynamicKubernetesObject oldObj = entry.getValue();

        if (oldObj == null) {
          api.delete(spec.apiVersion(), spec.kind(), spec.namespace(), spec.name());
          log.info("Removed K8s obj: {}", spec);
        } else {
          api.update(oldObj);
          log.info("Restored K8s obj: {}:{}", oldObj.getKind(), oldObj.getMetadata().getName());
        }
      } catch (SQLException e) {
        log.warn("Error restoring K8s YAML. This may be expected if the owner object was already deleted: {}", entry.getKey(), e);
      }
    }
    newToOldMap.clear();
  }


  // Private class intended to wrap necessary K8s information to deduplicate a resource
  private static class K8sSpec {
    private final String apiVersion;
    private final String kind;
    private final String namespace;
    private final String name;

    K8sSpec(KubernetesObject obj) {
      this.apiVersion = obj.getApiVersion();
      this.kind = obj.getKind();
      this.namespace = obj.getMetadata().getNamespace();
      this.name = obj.getMetadata().getName();
    }

    public String apiVersion() {
      return this.apiVersion;
    }

    public String kind() {
      return this.kind;
    }

    public String namespace() {
      return this.namespace;
    }

    public String name() {
      return this.name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      K8sSpec k8sSpec = (K8sSpec) o;
      return apiVersion.equals(k8sSpec.apiVersion)
          && kind.equals(k8sSpec.kind)
          && namespace.equals(k8sSpec.namespace)
          && name.equals(k8sSpec.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(apiVersion, kind, namespace, name);
    }
  }
}

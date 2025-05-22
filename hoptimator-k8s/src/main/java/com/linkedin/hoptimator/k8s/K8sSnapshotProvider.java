package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.SnapshotProvider;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// Grabs information from each K8s spec to snapshot the current state of the resource.
// On restore, the last state of each spec will be reapplied.
// Handles deletion if the resource previous did not exist.
public class K8sSnapshotProvider implements SnapshotProvider {
  private static final Logger log = LoggerFactory.getLogger(K8sSnapshotProvider.class);

  // Mapping of new Kubernetes yaml to existing object
  private final Map<String, DynamicKubernetesObject> newToOldMap = new HashMap<>();

  private K8sYamlApi api;

  // Used for testing purposes
  K8sSnapshotProvider(K8sYamlApi api) {
    this.api = api;
  }

  @Override
  public void snapshot(List<String> specs, Properties connectionProperties) throws SQLException {
    if (api == null) {
      this.api = new K8sYamlApi(K8sContext.create(connectionProperties));
    }
    for (String spec : specs) {
      DynamicKubernetesObject existing = api.get(spec);
      newToOldMap.put(spec, existing);
      log.info("Successfully snapshot K8s YAML: {}", spec);
    }
  }

  @Override
  public void restore() throws SQLException {
    if (api == null) {
      log.warn("K8sSnapshotProvider not initialized. Skipping restore operation.");
      return;
    }
    for (Map.Entry<String, DynamicKubernetesObject> entry : newToOldMap.entrySet()) {
      String newYaml = entry.getKey();
      DynamicKubernetesObject oldObj = entry.getValue();

      if (oldObj == null) {
        api.delete(newYaml);
        log.info("Removed K8s YAML: {}", newYaml);
      } else {
        api.update(oldObj);
        log.info("Restored K8s obj: {}:{}", oldObj.getKind(), oldObj.getMetadata().getName());
      }
    }
    newToOldMap.clear();
  }
}

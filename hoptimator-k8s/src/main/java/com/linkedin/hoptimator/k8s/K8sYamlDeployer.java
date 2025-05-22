package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.SnapshotService;
import io.kubernetes.client.util.Yaml;
import java.sql.SQLException;

import com.linkedin.hoptimator.Deployer;
import java.util.Collections;


public abstract class K8sYamlDeployer implements Deployer {

  private final K8sYamlApi api;
  private final K8sContext context;

  public K8sYamlDeployer(K8sContext context) {
    this.api = new K8sYamlApi(context);
    this.context = context;
  }

  @Override
  public void create() throws SQLException {
    for (String spec : specify()) {
      SnapshotService.snapshot(Collections.singletonList(Yaml.dump(spec)), context.connectionProperties());
      api.create(spec);
    }
  }

  @Override
  public void delete() throws SQLException {
    for (String spec : specify()) {
      SnapshotService.snapshot(Collections.singletonList(Yaml.dump(spec)), context.connectionProperties());
      api.delete(spec);
    }
  }

  @Override
  public void update() throws SQLException {
    for (String spec : specify()) {
      SnapshotService.snapshot(Collections.singletonList(Yaml.dump(spec)), context.connectionProperties());
      api.update(spec);
    }
  }
}

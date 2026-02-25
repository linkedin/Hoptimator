package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.TableSource;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.View;

public class K8sDeployerProvider implements DeployerProvider {

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    List<Deployer> list = new ArrayList<>();
    K8sContext context = K8sContext.create(connection);
    if (obj instanceof MaterializedView) {
      // K8sMaterializedViewDeployer also deploys a View.
      list.add(new K8sMaterializedViewDeployer((MaterializedView) obj, context));
    } else if (obj instanceof View) {
      list.add(new K8sViewDeployer((View) obj, false, context));
    } else if (obj instanceof Job) {
      list.add(new K8sJobDeployer((Job) obj, context));
    } else if (obj instanceof Source) {
      list.add(new K8sSourceDeployer((Source) obj, context));
      if (obj instanceof TableSource) {
        list.add(new K8sTableDeployer((TableSource) obj, context));
      }
    } else if (obj instanceof Trigger) {
      list.add(new K8sTriggerDeployer((Trigger) obj, context));
    }

    return list;
  }

  @Override
  public int priority() {
    return 1;
  }
}

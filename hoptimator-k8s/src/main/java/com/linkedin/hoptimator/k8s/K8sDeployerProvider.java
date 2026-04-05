package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
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
      if (!(obj instanceof Sink)) {
        // Sets up a table provisioning job for the source.
        // The job would be a no-op if the source is already provisioned.
        list.add(new K8sJobDeployer(jobFromSource((Source) obj), context));
      }
    } else if (obj instanceof Trigger) {
      list.add(new K8sTriggerDeployer((Trigger) obj, context));
    }

    return list;
  }

  private static Job jobFromSource(Source source) {
    Sink sink = new Sink(source.database(), source.path(), source.options());
    Map<String, ThrowingFunction<SqlDialect, String>> lazyEvals = new HashMap<>();
    lazyEvals.put("sql", dialect -> null);
    lazyEvals.put("query", dialect -> null);
    lazyEvals.put("fieldMap", dialect -> null);
    return new Job(source.table(), Collections.emptySet(), sink, lazyEvals);
  }

  @Override
  public int priority() {
    return 1;
  }
}

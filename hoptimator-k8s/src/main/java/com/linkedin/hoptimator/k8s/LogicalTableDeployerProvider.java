package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;


/**
 * Activates {@link K8sLogicalTableDeployer} for sources backed by a logical Database CRD.
 *
 * <p>A Database CRD is considered "logical" when its {@code spec.url} starts with
 * {@code jdbc:logical://}. This provider checks whether the source's database name
 * matches the {@code spec.schema} of any such CRD.
 *
 * <p>Priority {@code 0} ensures this provider runs before the generic K8s deployer
 * (priority {@code 1}), so logical tables are handled here and not by the default handler.
 */
public class LogicalTableDeployerProvider implements DeployerProvider {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployerProvider.class);

  private static final String LOGICAL_URL_PREFIX = "jdbc:logical://";

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    if (!(obj instanceof Source)) {
      return Collections.emptyList();
    }
    Source source = (Source) obj;
    K8sContext context = K8sContext.create(connection);

    // Look for a Database CRD whose spec.schema matches source.database()
    // AND whose spec.url starts with jdbc:logical://
    try {
      K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
          new K8sApi<>(context, K8sApiEndpoints.DATABASES);
      Collection<V1alpha1Database> databases = dbApi.list();

      for (V1alpha1Database db : databases) {
        if (db.getSpec() == null) {
          continue;
        }
        String schema = db.getSpec().getSchema();
        String url = db.getSpec().getUrl();
        if (url != null && url.startsWith(LOGICAL_URL_PREFIX)
            && source.database().equalsIgnoreCase(schema)) {
          log.debug("LogicalTableDeployerProvider activating for source {} (matched DB CRD {})",
              source, db.getMetadata() != null ? db.getMetadata().getName() : "<unknown>");
          return List.of(new K8sLogicalTableDeployer(source, context));
        }
      }
    } catch (SQLException e) {
      log.warn("Failed to list Database CRDs while checking for logical tables: {}", e.getMessage());
    }

    return Collections.emptyList();
  }

  @Override
  public int priority() {
    // Lower numeric priority = runs first; 0 beats the generic K8sDeployerProvider at 1
    return 0;
  }
}

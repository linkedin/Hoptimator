package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Provides {@link MySqlDeployer} instances for MySQL-backed tables.
 *
 * <p>Detection uses {@link DeploymentContext#databaseProperties} to resolve the source's
 * {@code Database} config and checks whether its connection URL starts with
 * {@code jdbc:mysql-hoptimator://}. This is Calcite-free: it works identically whether the config
 * comes from the Calcite catalog (SQL path) or from a {@code Database} CRD (direct path).
 */
public class MySqlDeployerProvider implements DeployerProvider {

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, DeploymentContext context) {
    List<Deployer> deployers = new ArrayList<>();
    if (obj instanceof Source) {
      Source source = (Source) obj;

      Properties properties = context.databaseProperties(
          source.catalog(), source.schema(), MySqlDriver.CONNECTION_PREFIX);

      if (properties == null) {
        return deployers;
      }

      deployers.add(new MySqlDeployer(source, properties, context));
    }
    return deployers;
  }

  @Override
  public int priority() {
    return 2;
  }
}

package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Provides {@link MySqlDeployer} instances for MySQL-backed tables.
 *
 * <p>Detection works by looking up the source's schema in the Calcite connection,
 * checking if it is a MySQL schema (TableSchema).
 */
public class MySqlDeployerProvider implements DeployerProvider {

  private static final Logger log = LoggerFactory.getLogger(MySqlDeployerProvider.class);

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    List<Deployer> deployers = new ArrayList<>();
    if (obj instanceof Source) {
      Source source = (Source) obj;

      String catalog = source.catalog();
      if (catalog == null || !catalog.equalsIgnoreCase(MySqlDriver.CATALOG_NAME)) {
        return deployers;
      }

      Properties properties = DeployerUtils.extractPropertiesFromJdbcSchema(
          source.catalog(),
          source.schema(),
          connection,
          MySqlDriver.CONNECTION_PREFIX,
          log);
      if (properties == null) {
        return deployers;
      }

      if (!(connection instanceof HoptimatorConnection)) {
        log.error("Connection is not a HoptimatorConnection, cannot create MySqlDeployer");
        return deployers;
      }

      deployers.add(new MySqlDeployer(source, properties, (HoptimatorConnection) connection));
    }
    return deployers;
  }

  @Override
  public int priority() {
    return 2;
  }
}

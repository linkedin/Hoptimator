package com.linkedin.hoptimator.venice;

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


public class VeniceDeployerProvider implements DeployerProvider {

  private static final Logger log = LoggerFactory.getLogger(VeniceDeployerProvider.class);

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    List<Deployer> deployers = new ArrayList<>();
    if (obj instanceof Source && connection instanceof HoptimatorConnection) {
      Source source = (Source) obj;

      String database = source.database();
      if (database == null || !database.equalsIgnoreCase(VeniceDriver.CATALOG_NAME)) {
        return deployers;
      }

      Properties properties = DeployerUtils.extractPropertiesFromJdbcSchema(
          source.catalog(),
          source.schema(),
          connection,
          VeniceDriver.CONNECTION_PREFIX,
          log);
      if (properties == null) {
        return deployers;
      }

      deployers.add(new VeniceDeployer(source, properties, (HoptimatorConnection) connection));
    }

    return deployers;
  }

  @Override
  public int priority() {
    return 2;
  }
}

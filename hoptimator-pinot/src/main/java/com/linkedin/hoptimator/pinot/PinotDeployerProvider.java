package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class PinotDeployerProvider implements DeployerProvider {

  @Override
  public <T extends Deployable> Collection<Deployer> deployers(T obj, DeploymentContext context) throws SQLException {
    List<Deployer> deployers = new ArrayList<>();
    if (obj instanceof Source) {
      Source source = (Source) obj;

      Properties properties = context.databaseProperties(
          source.catalog(), source.schema(), PinotDriver.CONNECTION_PREFIX);

      if (properties == null) {
        return deployers;
      }

      deployers.add(new PinotDeployer(source, properties, context));
    }

    return deployers;
  }

  @Override
  public int priority() {
    return 2;
  }
}

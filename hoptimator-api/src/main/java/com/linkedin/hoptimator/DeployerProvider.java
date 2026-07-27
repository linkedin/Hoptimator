package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Collection;


public interface DeployerProvider {

  /** Find deployers capable of deploying the obj. */
  <T extends Deployable> Collection<Deployer> deployers(T obj, DeploymentContext context) throws SQLException;

  /** A DeployerProvider with lower priority will execute first */
  int priority();
}

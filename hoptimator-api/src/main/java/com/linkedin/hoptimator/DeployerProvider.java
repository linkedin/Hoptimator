package com.linkedin.hoptimator;

import java.sql.Connection;
import java.util.Collection;


public interface DeployerProvider {

  /** Find deployers capable of deploying the obj. */
  <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection);

  /** A DeployerProvider with lower priority will execute first */
  int priority();
}

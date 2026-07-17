package com.linkedin.hoptimator;

import java.util.Collection;


public interface DeployerProvider {

  /** Find deployers capable of deploying the obj. */
  <T extends Deployable> Collection<Deployer> deployers(T obj, DeploymentContext context);

  /** A DeployerProvider with lower priority will execute first */
  int priority();
}

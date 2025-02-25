package com.linkedin.hoptimator;

import java.util.Collection;
import java.util.Properties;


public interface DeployerProvider {

  /** Find deployers capable of deploying the obj. */
  <T extends Deployable> Collection<Deployer> deployers(T obj, Properties connectionProperties);
}

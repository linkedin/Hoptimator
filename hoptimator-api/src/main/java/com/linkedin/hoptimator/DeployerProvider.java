package com.linkedin.hoptimator;

import java.util.Collection;
import java.util.Properties;


public interface DeployerProvider {

  <T> Collection<Deployer<T>> deployers(Class<T> clazz, Properties connectionProperties);
}

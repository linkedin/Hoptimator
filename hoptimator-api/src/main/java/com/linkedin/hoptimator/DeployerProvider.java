package com.linkedin.hoptimator;

import java.util.Collection;


public interface DeployerProvider {

  <T> Collection<Deployer<T>> deployers(Class<T> clazz);
}

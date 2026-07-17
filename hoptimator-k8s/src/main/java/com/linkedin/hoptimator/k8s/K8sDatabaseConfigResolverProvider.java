package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.DatabaseConfigResolver;
import com.linkedin.hoptimator.jdbc.DatabaseConfigResolverProvider;

import java.util.Properties;


/** Supplies a {@link K8sDatabaseConfigResolver} so the direct path resolves config from CRDs. */
public class K8sDatabaseConfigResolverProvider implements DatabaseConfigResolverProvider {

  @Override
  public DatabaseConfigResolver resolver(Properties connectionProperties) {
    return new K8sDatabaseConfigResolver(connectionProperties);
  }

  @Override
  public int priority() {
    return 1;
  }
}

package com.linkedin.hoptimator;

import java.util.Collection;
import java.util.Properties;


public interface ConnectorProvider {

  <T> Collection<Connector<T>> connectors(Class<T> clazz, Properties connectionProperties);
}

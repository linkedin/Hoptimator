package com.linkedin.hoptimator;

import java.util.Collection;


public interface ConnectorProvider {

  <T> Collection<Connector<T>> connectors(Class<T> clazz);
}

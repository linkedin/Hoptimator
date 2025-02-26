package com.linkedin.hoptimator;

import java.util.Collection;
import java.util.Properties;


public interface ConnectorProvider {

  /** Find connectors capable of configuring data plane connectors for the obj. */
  <T> Collection<Connector> connectors(T obj, Properties connectionProperties);
}

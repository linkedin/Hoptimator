package com.linkedin.hoptimator;

import java.util.Collection;


public interface ConnectorProvider {

  /** Find connectors capable of configuring data plane connectors for the obj. */
  <T> Collection<Connector> connectors(T obj, DeploymentContext context);
}

package com.linkedin.hoptimator;

import java.util.Map;

/**
 * Interface for tables that carry connector configuration.
 * This lets planner rules detect K8s-stored tables without
 * depending on hoptimator-k8s.
 */
public interface ConnectorConfigurable {

  /** Returns the connector options (WITH clause values). */
  Map<String, String> connectorOptions();

  /** Returns the internal database name for this table. */
  String databaseName();
}

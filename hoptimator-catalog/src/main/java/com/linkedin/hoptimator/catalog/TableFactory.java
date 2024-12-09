package com.linkedin.hoptimator.catalog;

import java.util.Collections;

import org.apache.calcite.rel.type.RelDataType;


/** Constructs a HopTable */
public interface TableFactory {
  HopTable table(String database, String name, RelDataType rowType);

  /** Construct a ConnectorFactory */
  static ConnectorFactory connector(ConfigProvider configs) {
    return new ConnectorFactory(configs);
  }

  /** Construct a ConnectorFactory */
  static ConnectorFactory connector(ConfigProvider configs, ResourceProvider resources) {
    return new ConnectorFactory(configs, resources);
  }

  /** A TableFactory which is implemented as a simple connector. */
  class ConnectorFactory implements TableFactory {
    private final ConfigProvider configProvider;
    private final ResourceProvider resourceProvider;

    public ConnectorFactory(ConfigProvider configProvider, ResourceProvider resourceProvider) {
      this.configProvider = configProvider;
      this.resourceProvider = resourceProvider;
    }

    public ConnectorFactory(ConfigProvider configProvider) {
      this(configProvider, x -> Collections.emptyList());
    }

    @Override
    public HopTable table(String database, String name, RelDataType rowType) {
      return new HopTable(database, name, rowType, resourceProvider.readResources(name),
          resourceProvider.writeResources(name),
          ScriptImplementor.empty().connector(database, name, rowType, configProvider.config(name)));
    }
  }
}

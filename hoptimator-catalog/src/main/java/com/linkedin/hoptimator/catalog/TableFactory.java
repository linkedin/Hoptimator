package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;

import java.util.Collections;

/** Constructs a HopTable */
public interface TableFactory {
  HopTable table(String database, String name, RelDataType rowType);

  /** Construct a ConnectorFactory */
  static ConnectorFactory connector(ConfigProvider configProvider) {
    return new ConnectorFactory(configProvider);
  }

  /** Construct a ConnectorFactory */
  static ConnectorFactory connector(ConfigProvider configProvider,
      ResourceProvider resourceProvider) {
    return new ConnectorFactory(configProvider, resourceProvider);
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
      return new HopTable(database, name, rowType, resourceProvider.resources(name),
        ScriptImplementor.empty().connector(database, name, rowType, configProvider.config(name)));
    }
  }
}

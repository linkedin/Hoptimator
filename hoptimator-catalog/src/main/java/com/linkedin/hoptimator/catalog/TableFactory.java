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

  /** A TableFactory which is implemented as a simple connector. */
  class ConnectorFactory implements TableFactory {
    private final ConfigProvider configProvider;

    public ConnectorFactory(ConfigProvider configProvider) {
      this.configProvider = configProvider;
    }

    @Override
    public HopTable table(String database, String name, RelDataType rowType) {
      return new HopTable(database, name, rowType, () -> Collections.emptyList(),
        ScriptImplementor.empty().connector(database, name, rowType, configProvider.config(name)));
    }
  }
}

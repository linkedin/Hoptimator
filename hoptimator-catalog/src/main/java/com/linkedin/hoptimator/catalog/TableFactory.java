package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;

import java.util.Collections;

public interface TableFactory {
  AdapterTable table(String database, String name, RelDataType rowType);

  static ConnectorFactory connector(ConfigProvider configProvider) {
    return new ConnectorFactory(configProvider);
  }

  class ConnectorFactory implements TableFactory {
    private final ConfigProvider configProvider;

    public ConnectorFactory(ConfigProvider configProvider) {
      this.configProvider = configProvider;
    }

    @Override
    public AdapterTable table(String database, String name, RelDataType rowType) {
      return new AdapterTable(database, name, rowType, () -> Collections.emptyList(),
        ScriptImplementor.empty().connector(database, name, rowType, configProvider.config(name)));
    }
  }
}

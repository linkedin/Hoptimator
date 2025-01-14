package com.linkedin.hoptimator.k8s;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;


/** Built-in K8s metadata tables */
public class K8sMetadata extends AbstractSchema {

  private final Map<String, Table> tableMap = new HashMap<>();
  private final K8sDatabaseTable databaseTable;
  private final K8sEngineTable engineTable;
  private final K8sViewTable viewTable;

  public K8sMetadata(K8sContext context) {
    this.engineTable = new K8sEngineTable(context);
    this.databaseTable = new K8sDatabaseTable(context, engineTable);
    this.viewTable = new K8sViewTable(context);
    tableMap.put("DATABASES", databaseTable);
    tableMap.put("ENGINES", engineTable);
    tableMap.put("VIEWS", viewTable);
  }

  public K8sDatabaseTable databaseTable() {
    return databaseTable;
  }

  public K8sEngineTable engineTable() {
    return engineTable;
  }

  public K8sViewTable viewTable() {
    return viewTable;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}

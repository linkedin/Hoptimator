package com.linkedin.hoptimator.k8s;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;


/** Built-in K8s metadata tables */
public class K8sMetadata extends AbstractSchema {

  private final Map<String, Table> tableMap = new HashMap<>();
  private final K8sDatabaseTable databaseTable;
  private final K8sEngineTable engineTable;
  private final K8sPipelineTable pipelineTable;
  private final K8sViewTable viewTable;

  public K8sMetadata(HoptimatorConnection connection, K8sContext context) {
    K8sPipelineElementApi pipelineElementApi = new K8sPipelineElementApi(context);
    K8sPipelineElementMapApi pipelineElementMapApi = new K8sPipelineElementMapApi(pipelineElementApi);

    this.engineTable = new K8sEngineTable(context);
    this.databaseTable = new K8sDatabaseTable(context, engineTable);
    this.pipelineTable = new K8sPipelineTable(context);
    this.viewTable = new K8sViewTable(connection, context);
    tableMap.put("DATABASES", databaseTable);
    tableMap.put("ENGINES", engineTable);
    tableMap.put("PIPELINES", pipelineTable);
    tableMap.put("PIPELINE_ELEMENTS",  new K8sPipelineElementTable(pipelineElementApi));
    tableMap.put("PIPELINE_ELEMENT_MAP",  new K8sPipelineElementMapTable(pipelineElementMapApi));
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

  public K8sPipelineTable pipelineTable() {
    return pipelineTable;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}

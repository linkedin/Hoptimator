package com.linkedin.hoptimator.k8s;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;


/** Built-in K8s metadata tables */
public class K8sMetadata extends AbstractSchema {

  private final HoptimatorConnection connection;
  private final K8sContext context;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public K8sMetadata(HoptimatorConnection connection, K8sContext context) {
    this.connection = connection;
    this.context = context;
  }

  public K8sDatabaseTable databaseTable() {
    return (K8sDatabaseTable) tables().get("DATABASES");
  }

  public K8sViewTable viewTable() {
    return (K8sViewTable) tables().get("VIEWS");
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() {
        Map<String, Table> tableMap = new HashMap<>();
        K8sPipelineElementApi pipelineElementApi = new K8sPipelineElementApi(context);
        K8sPipelineElementMapApi pipelineElementMapApi = new K8sPipelineElementMapApi(pipelineElementApi);

        K8sEngineTable engineTable = new K8sEngineTable(context);
        tableMap.put("DATABASES", new K8sDatabaseTable(context, engineTable));
        tableMap.put("ENGINES", engineTable);
        tableMap.put("PIPELINES", new K8sPipelineTable(context));
        tableMap.put("PIPELINE_ELEMENTS",  new K8sPipelineElementTable(pipelineElementApi));
        tableMap.put("PIPELINE_ELEMENT_MAP",  new K8sPipelineElementMapTable(pipelineElementMapApi));
        tableMap.put("TABLE_TRIGGERS",  new K8sTableTriggerTable(context));
        tableMap.put("VIEWS", new K8sViewTable(connection, context));
        return tableMap;
      }

      @Override
      protected @Nullable Table loadTable(String name) {
        switch (name) {
          case "DATABASES":
            K8sEngineTable engineTable = new K8sEngineTable(context);
            return new K8sDatabaseTable(context, engineTable);
          case "ENGINES":
            return new K8sEngineTable(context);
          case "PIPELINES":
            return new K8sPipelineTable(context);
          case "PIPELINE_ELEMENTS":
            K8sPipelineElementApi pipelineElementApi = new K8sPipelineElementApi(context);
            return new K8sPipelineElementTable(pipelineElementApi);
          case "PIPELINE_ELEMENT_MAP":
            K8sPipelineElementApi api = new K8sPipelineElementApi(context);
            K8sPipelineElementMapApi mapApi = new K8sPipelineElementMapApi(api);
            return new K8sPipelineElementMapTable(mapApi);
          case "TABLE_TRIGGERS":
            return new K8sTableTriggerTable(context);
          case "VIEWS":
            return new K8sViewTable(connection, context);
          default:
            return null;
        }
      }

      @Override
      protected String getSchemaDescription() {
        return "K8s Metadata Schema";
      }
    });
  }
}

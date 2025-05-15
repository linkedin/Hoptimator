package com.linkedin.hoptimator.k8s;

import java.util.Optional;

import org.apache.calcite.schema.Schema;

import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;


public class K8sTableTriggerTable extends K8sTable<V1alpha1TableTrigger, V1alpha1TableTriggerList, K8sTableTriggerTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String SCHEMA;
    public String TABLE;
    public String TIMESTAMP;
    public String WATERMARK;

    public Row(String name, String schema, String table, String timestamp, String watermark) {
      this.NAME = name;
      this.SCHEMA = schema;
      this.TABLE = table;
      this.TIMESTAMP = timestamp;
      this.WATERMARK = watermark;
    }
  }
  // CHECKSTYLE:ON

  public K8sTableTriggerTable(K8sContext context) {
    super(context, K8sApiEndpoints.TABLE_TRIGGERS, Row.class);
  }

  @Override
  public Row toRow(V1alpha1TableTrigger obj) {
    return new Row(obj.getMetadata().getName(), obj.getSpec().getSchema(), obj.getSpec().getTable(),
        Optional.ofNullable(obj.getStatus())
            .flatMap(x -> Optional.ofNullable(x.getTimestamp()))
            .map(x -> x.toString()).orElse(null),
        Optional.ofNullable(obj.getStatus())
            .flatMap(x -> Optional.ofNullable(x.getWatermark()))
            .map(x -> x.toString()).orElse(null));
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}

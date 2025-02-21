package com.linkedin.hoptimator.k8s;

import org.apache.calcite.schema.Schema;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;


public class K8sPipelineTable extends K8sTable<V1alpha1Pipeline, V1alpha1PipelineList, K8sPipelineTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String STATUS;

    public Row(String name, String status) {
      this.NAME = name;
      this.STATUS = status;
    }

    @Override
    public String toString() {
      return String.join("\t", NAME, STATUS);
    }
  }
  // CHECKSTYLE:ON

  public K8sPipelineTable(K8sContext context) {
    super(context, K8sApiEndpoints.PIPELINES, Row.class);
  }

  @Override
  public Row toRow(V1alpha1Pipeline obj) {
    return new Row(obj.getMetadata().getName(), obj.getStatus().getMessage());
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}

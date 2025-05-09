package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;
import org.apache.calcite.schema.Schema;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;


public class K8sPipelineTable extends K8sTable<V1alpha1Pipeline, V1alpha1PipelineList, K8sPipelineTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public boolean READY;
    public boolean FAILED;
    public String MESSAGE;

    public Row(String name, boolean ready, boolean failed, String message) {
      this.NAME = name;
      this.READY = ready;
      this.FAILED = failed;
      this.MESSAGE = message;
    }

    @Override
    public String toString() {
      return String.join("\t", NAME, String.valueOf(READY), String.valueOf(FAILED), MESSAGE);
    }
  }
  // CHECKSTYLE:ON

  public K8sPipelineTable(K8sContext context) {
    super(context, K8sApiEndpoints.PIPELINES, Row.class);
  }

  @Override
  public Row toRow(V1alpha1Pipeline obj) {
    V1alpha1PipelineStatus status = obj.getStatus();
    return new Row(obj.getMetadata().getName(), status.getReady(), status.getFailed(), status.getMessage());
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}

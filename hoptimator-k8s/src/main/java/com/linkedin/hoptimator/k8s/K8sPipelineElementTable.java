package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.util.RemoteTable;
import org.apache.calcite.schema.Schema;


public class K8sPipelineElementTable extends RemoteTable<K8sPipelineElement, K8sPipelineElementTable.Row> {

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

  public K8sPipelineElementTable(K8sPipelineElementApi pipelineElementApi) {
    super(pipelineElementApi, Row.class);
  }

  @Override
  public Row toRow(K8sPipelineElement k8sPipelineElement) {
    K8sPipelineElementStatus status = k8sPipelineElement.status();
    return new Row(status.getName(), status.isReady(), status.isFailed(), status.getMessage());
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}

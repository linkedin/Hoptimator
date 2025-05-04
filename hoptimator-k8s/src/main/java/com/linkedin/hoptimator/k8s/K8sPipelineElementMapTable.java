package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.RemoteTable;
import org.apache.calcite.schema.Schema;


/** Provides n:n mapping between pipeline elements and their pipelines. */
public class K8sPipelineElementMapTable
    extends RemoteTable<K8sPipelineElementMapEntry, K8sPipelineElementMapTable.Row> {

  // CHECKSTYLE:OFF
  public static class Row {
    public String ELEMENT_NAME;
    public String PIPELINE_NAME;

    public Row(String elementName, String pipelineName) {
      this.ELEMENT_NAME = elementName;
      this.PIPELINE_NAME = pipelineName;
    }

    @Override
    public String toString() {
      return String.join("\t", ELEMENT_NAME, PIPELINE_NAME);
    }
  }
  // CHECKSTYLE:ON

  public K8sPipelineElementMapTable(K8sPipelineElementMapApi pipelineElementMapApi) {
    super(pipelineElementMapApi, Row.class);
  }

  @Override
  public Row toRow(K8sPipelineElementMapEntry k8sDynamicPipelineElement) {
    return new Row(k8sDynamicPipelineElement.getElementName(), k8sDynamicPipelineElement.getPipelineName());
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}

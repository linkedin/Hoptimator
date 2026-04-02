package com.linkedin.hoptimator.k8s;

import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class K8sPipelineElementMapTableTest {

  @Test
  void toRowMapsFields() {
    K8sPipelineElementMapEntry entry = new K8sPipelineElementMapEntry("elem-1", "pipeline-1");

    K8sPipelineElementMapTable table = new K8sPipelineElementMapTable(null);
    K8sPipelineElementMapTable.Row row = table.toRow(entry);

    assertEquals("elem-1", row.ELEMENT_NAME);
    assertEquals("pipeline-1", row.PIPELINE_NAME);
  }

  @Test
  void rowToStringContainsFields() {
    K8sPipelineElementMapTable.Row row = new K8sPipelineElementMapTable.Row("elem", "pipe");
    String str = row.toString();
    assertEquals("elem\tpipe", str);
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sPipelineElementMapTable table = new K8sPipelineElementMapTable(null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }
}

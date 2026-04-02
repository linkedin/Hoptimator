package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sPipelineTableTest {

  @Test
  void rowToStringContainsAllFields() {
    K8sPipelineTable.Row row = new K8sPipelineTable.Row("my-pipeline", true, false, "Ready");
    String str = row.toString();
    assertTrue(str.contains("my-pipeline"));
    assertTrue(str.contains("true"));
    assertTrue(str.contains("false"));
    assertTrue(str.contains("Ready"));
  }

  @Test
  void rowFieldsAreCorrect() {
    K8sPipelineTable.Row row = new K8sPipelineTable.Row("pipeline-1", false, true, "Failed");
    assertEquals("pipeline-1", row.NAME);
    assertFalse(row.READY);
    assertTrue(row.FAILED);
    assertEquals("Failed", row.MESSAGE);
  }

  @Test
  void toRowMapsStatusFields() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("test-pipeline"))
        .status(new V1alpha1PipelineStatus().ready(true).failed(false).message("All good"));

    K8sPipelineTable table = new K8sPipelineTable(null);
    K8sPipelineTable.Row row = table.toRow(pipeline);

    assertEquals("test-pipeline", row.NAME);
    assertTrue(row.READY);
    assertFalse(row.FAILED);
    assertEquals("All good", row.MESSAGE);
  }

  @Test
  void toRowWithNullReadyAndFailed() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("p"))
        .status(new V1alpha1PipelineStatus().ready(null).failed(null).message("pending"));

    K8sPipelineTable table = new K8sPipelineTable(null);
    K8sPipelineTable.Row row = table.toRow(pipeline);

    assertFalse(row.READY);
    assertFalse(row.FAILED);
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sPipelineTable table = new K8sPipelineTable(null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }
}

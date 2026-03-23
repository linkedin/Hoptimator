package com.linkedin.hoptimator.k8s;

import java.util.Collections;

import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class K8sPipelineElementTableTest {

  @Test
  void toRowMapsAllFields() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline1"));
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("elem1", true, false, "Ready");
    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, Collections.singletonMap("key", "value"));

    K8sPipelineElementTable table = new K8sPipelineElementTable(null);
    K8sPipelineElementTable.Row row = table.toRow(element);

    assertEquals("elem1", row.NAME);
    assertTrue(row.READY);
    assertFalse(row.FAILED);
    assertEquals("Ready", row.MESSAGE);
    assertNotNull(row.CONFIGS);
    assertTrue(row.CONFIGS.contains("key"));
    assertTrue(row.CONFIGS.contains("value"));
  }

  @Test
  void toRowWithFailedStatus() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline1"));
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("elem2", false, true, "Error occurred");
    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, Collections.emptyMap());

    K8sPipelineElementTable table = new K8sPipelineElementTable(null);
    K8sPipelineElementTable.Row row = table.toRow(element);

    assertFalse(row.READY);
    assertTrue(row.FAILED);
    assertEquals("Error occurred", row.MESSAGE);
  }

  @Test
  void rowToStringContainsFields() {
    K8sPipelineElementTable.Row row = new K8sPipelineElementTable.Row("elem", true, false, "msg", "{}");
    String str = row.toString();
    assertTrue(str.contains("elem"));
    assertTrue(str.contains("true"));
    assertTrue(str.contains("msg"));
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sPipelineElementTable table = new K8sPipelineElementTable(null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }
}

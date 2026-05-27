package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sTableTriggerTableTest {

  @Test
  void toRowMapsAllFields() {
    OffsetDateTime timestamp = OffsetDateTime.now();
    OffsetDateTime watermark = OffsetDateTime.now().minusHours(1);
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("my-trigger"))
        .spec(new V1alpha1TableTriggerSpec().schema("sch").table("tbl").paused(true))
        .status(new V1alpha1TableTriggerStatus().timestamp(timestamp).watermark(watermark));

    K8sTableTriggerTable table = new K8sTableTriggerTable(null);
    K8sTableTriggerTable.Row row = table.toRow(trigger);

    assertEquals("my-trigger", row.NAME);
    assertEquals("sch", row.SCHEMA);
    assertEquals("tbl", row.TABLE);
    assertTrue(row.PAUSED);
    assertEquals(timestamp.toString(), row.TIMESTAMP);
    assertEquals(watermark.toString(), row.WATERMARK);
  }

  @Test
  void toRowWithNullStatus() {
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("trigger"))
        .spec(new V1alpha1TableTriggerSpec().schema("sch").table("tbl").paused(false))
        .status(null);

    K8sTableTriggerTable table = new K8sTableTriggerTable(null);
    K8sTableTriggerTable.Row row = table.toRow(trigger);

    assertEquals("trigger", row.NAME);
    assertNull(row.TIMESTAMP);
    assertNull(row.WATERMARK);
  }

  @Test
  void toRowWithNullTimestampAndWatermark() {
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("trigger"))
        .spec(new V1alpha1TableTriggerSpec().schema("sch").table("tbl"))
        .status(new V1alpha1TableTriggerStatus().timestamp(null).watermark(null));

    K8sTableTriggerTable table = new K8sTableTriggerTable(null);
    K8sTableTriggerTable.Row row = table.toRow(trigger);

    assertNull(row.TIMESTAMP);
    assertNull(row.WATERMARK);
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sTableTriggerTable table = new K8sTableTriggerTable(null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }
}

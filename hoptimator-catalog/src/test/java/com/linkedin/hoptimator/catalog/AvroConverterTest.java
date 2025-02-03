package com.linkedin.hoptimator.catalog;

import org.apache.avro.Schema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Litmus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class AvroConverterTest {

  @Test
  public void convertsNestedSchemas() {
    // CHECKSTYLE:OFF
    String schemaString =
        "{\"type\":\"record\",\"name\":\"E\",\"namespace\":\"ns\",\"fields\":[{\"name\":\"h\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"H\",\"namespace\":\"ns\",\"fields\":[{\"name\":\"A\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"A\",\"fields\":[]}]}]}]}]}";
    // CHECKSTYLE:ON

    Schema avroSchema1 = (new Schema.Parser()).parse(schemaString);
    RelDataType rel1 = AvroConverter.rel(avroSchema1);
    assertEquals(rel1.toString(), rel1.getFieldCount(), avroSchema1.getFields().size());
    assertNotNull(rel1.toString(), rel1.getField("h", false, false));
    RelDataType rel2 = rel1.getField("h", false, false).getType();
    assertTrue(rel2.toString(), rel2.isNullable());
    Schema avroSchema2 = avroSchema1.getField("h").schema().getTypes().get(1);
    assertEquals(rel2.toString(), rel2.getFieldCount(), avroSchema2.getFields().size());
    assertNotNull(rel2.toString(), rel2.getField("A", false, false));
    RelDataType rel3 = rel2.getField("A", false, false).getType();
    assertTrue(rel3.toString(), rel3.isNullable());
    Schema avroSchema3 = avroSchema2.getField("A").schema().getTypes().get(1);
    assertEquals(rel3.toString(), rel3.getFieldCount(), avroSchema3.getFields().size());
    Schema avroSchema4 = AvroConverter.avro("NS", "R", rel1);
    assertFalse("!avroSchema4.isNullable()", avroSchema4.isNullable());
    assertEquals(avroSchema4.toString(), avroSchema4.getFields().size(), rel1.getFieldCount());
    Schema avroSchema5 = AvroConverter.avro("NS", "R", rel2);
    assertTrue("avroSchema5.isNullable()", avroSchema5.isNullable());
    assertEquals(avroSchema5.toString(), avroSchema5.getTypes().get(1).getFields().size(), rel2.getFieldCount());
    Schema avroSchema6 = AvroConverter.avro("NS", "R", rel3);
    assertEquals(avroSchema6.toString(), avroSchema6.getTypes().get(1).getFields().size(), rel3.getFieldCount());
    RelDataType rel4 = AvroConverter.rel(avroSchema4);
    assertTrue("types match", RelOptUtil.eq("rel4", rel4, "rel1", rel1, Litmus.THROW));
  }

  @Test
  public void convertsNestedUnionSchemas() {
    String schemaString = "{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"ns\",\"fields\":[{\"name\":\"event\",\"type\":[{\"type\":\"record\",\"name\":\"record_event1\",\"fields\":[{\"name\":\"strField\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"record_event2\",\"fields\":[{\"name\":\"strField\",\"type\":\"string\"}]}]}]}";
    Schema avroSchema1 = (new Schema.Parser()).parse(schemaString);
    RelDataType rel1 = AvroConverter.rel(avroSchema1);
    assertEquals(rel1.toString(), rel1.getFieldCount(), avroSchema1.getFields().size());
    assertNotNull(rel1.toString(), rel1.getField("event", false, false));
    RelDataType rel2 = rel1.getField("event", false, false).getType();
    assertTrue(rel2.isStruct());
    Schema avroSchema2 = avroSchema1.getField("event").schema();
    assertEquals(rel2.toString(), rel2.getFieldCount(), avroSchema2.getTypes().size());
    RelDataType rel3 = rel2.getField("record_event1", false, false).getType();
    Schema avroSchema3 = avroSchema2.getTypes().get(0);
    assertEquals(rel3.toString(), rel3.getFieldCount(), avroSchema3.getFields().size());
    Schema avroSchema4 = AvroConverter.avro("NS", "R", rel1);
    assertFalse("!avroSchema4.isNullable()", avroSchema4.isNullable());
    assertEquals(avroSchema4.toString(), avroSchema4.getFields().size(), rel1.getFieldCount());
  }
}

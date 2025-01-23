package com.linkedin.hoptimator.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.util.planner.ScriptImplementor;


public class TestDataTypeUtils {

  @Test
  public void flattenUnflatten() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder1 = new RelDataTypeFactory.Builder(typeFactory);
    builder1.add("QUX", SqlTypeName.VARCHAR);
    builder1.add("QIZ", SqlTypeName.VARCHAR);
    RelDataTypeFactory.Builder builder2 = new RelDataTypeFactory.Builder(typeFactory);
    builder2.add("BAZ", SqlTypeName.VARCHAR);
    RelDataTypeFactory.Builder builder3 = new RelDataTypeFactory.Builder(typeFactory);
    builder3.add("FOO", builder1.build());
    builder3.add("BAR", builder2.build());
    RelDataType rowType = builder3.build();
    Assertions.assertEquals(2, rowType.getFieldList().size());
    RelDataType flattenedType = DataTypeUtils.flatten(rowType, typeFactory);
    Assertions.assertEquals(3, flattenedType.getFieldList().size());
    List<String> flattenedNames = flattenedType.getFieldList().stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
    Assertions.assertIterableEquals(Arrays.asList("FOO$QUX", "FOO$QIZ", "BAR$BAZ"),
        flattenedNames);
    RelDataType unflattenedType = DataTypeUtils.unflatten(flattenedType, typeFactory);
    RelOptUtil.eq("original", rowType, "flattened-unflattened", unflattenedType, Litmus.THROW);
    String originalConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        rowType, Collections.emptyMap()).sql();
    String unflattenedConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        unflattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals(originalConnector, unflattenedConnector,
        "Flattening and unflattening data types should have no impact on connector");
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `S`.`T1` (`FOO` ROW(`QUX` VARCHAR, "
        + "`QIZ` VARCHAR), `BAR` ROW(`BAZ` VARCHAR)) WITH ();", unflattenedConnector,
        "Flattened-unflattened connector should be correct");
  }

  @Test
  public void flattenUnflattenNestedArrays() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder1 = new RelDataTypeFactory.Builder(typeFactory);
    builder1.add("QUX", SqlTypeName.VARCHAR);
    builder1.add("QIZ", typeFactory.createArrayType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), -1));
    RelDataTypeFactory.Builder builder2 = new RelDataTypeFactory.Builder(typeFactory);
    builder2.add("BAZ", SqlTypeName.VARCHAR);
    RelDataTypeFactory.Builder builder3 = new RelDataTypeFactory.Builder(typeFactory);
    builder3.add("FOO", typeFactory.createArrayType(builder1.build(), -1));
    builder3.add("BAR", typeFactory.createArrayType(builder2.build(), -1));
    builder3.add("CAR", typeFactory.createArrayType(
        typeFactory.createSqlType(SqlTypeName.FLOAT), -1));
    RelDataType rowType = builder3.build();
    Assertions.assertEquals(3, rowType.getFieldList().size());
    RelDataType flattenedType = DataTypeUtils.flatten(rowType, typeFactory);
    Assertions.assertEquals(6, flattenedType.getFieldList().size());
    List<String> flattenedNames = flattenedType.getFieldList().stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
    Assertions.assertIterableEquals(Arrays.asList("FOO", "FOO$QUX", "FOO$QIZ", "BAR", "BAR$BAZ", "CAR"),
        flattenedNames);
    String flattenedConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        flattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `S`.`T1` ("
            + "`FOO` ANY ARRAY, `FOO_QUX` VARCHAR, `FOO_QIZ` VARCHAR ARRAY, "
            + "`BAR` ANY ARRAY, `BAR_BAZ` VARCHAR, "
            + "`CAR` FLOAT ARRAY) WITH ();", flattenedConnector,
        "Flattened connector should have simplified arrays");

    RelDataType unflattenedType = DataTypeUtils.unflatten(flattenedType, typeFactory);
    RelOptUtil.eq("original", rowType, "flattened-unflattened", unflattenedType, Litmus.THROW);
    String originalConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        rowType, Collections.emptyMap()).sql();
    String unflattenedConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        unflattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals(originalConnector, unflattenedConnector,
        "Flattening and unflattening data types should have no impact on connector");
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `S`.`T1` ("
            + "`FOO` ROW(`QUX` VARCHAR, `QIZ` VARCHAR ARRAY) ARRAY, "
            + "`BAR` ROW(`BAZ` VARCHAR) ARRAY, "
            + "`CAR` FLOAT ARRAY) WITH ();", unflattenedConnector,
        "Flattened-unflattened connector should be correct");
  }
}

package com.linkedin.hoptimator.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
    List<String> flattenedNames = flattenedType.getFieldList().stream().map(x -> x.getName())
        .collect(Collectors.toList());
    Assertions.assertIterableEquals(Arrays.asList(new String[]{"FOO$QUX", "FOO$QIZ", "BAR$BAZ"}),
        flattenedNames);
    RelDataType unflattenedType = DataTypeUtils.unflatten(flattenedType, typeFactory);
    RelOptUtil.eq("original", rowType, "flattened-unflattened", unflattenedType, Litmus.THROW);
    String originalConnector = new ScriptImplementor.ConnectorImplementor("T1",
        rowType, Collections.emptyMap()).sql();
    String unflattenedConnector = new ScriptImplementor.ConnectorImplementor("T1",
        unflattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals(originalConnector, unflattenedConnector,
        "Flattening and unflattening data types should have no impact on connector");
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `T1` (`FOO` ROW(`QUX` VARCHAR, "
        + "`QIZ` VARCHAR), `BAR` ROW(`BAZ` VARCHAR)) WITH ();", unflattenedConnector,
        "Flattened-unflattened connector should be correct");
  }

  @Test
  public void flattenNestedArrays() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder1 = new RelDataTypeFactory.Builder(typeFactory);
    builder1.add("QUX", SqlTypeName.VARCHAR);
    builder1.add("QIZ", SqlTypeName.VARCHAR);
    RelDataTypeFactory.Builder builder2 = new RelDataTypeFactory.Builder(typeFactory);
    builder2.add("BAZ", SqlTypeName.VARCHAR);
    RelDataTypeFactory.Builder builder3 = new RelDataTypeFactory.Builder(typeFactory);
    builder3.add("FOO", typeFactory.createArrayType(builder1.build(), -1));
    builder3.add("BAR", typeFactory.createArrayType(builder2.build(), -1));
    RelDataType rowType = builder3.build();
    Assertions.assertEquals(2, rowType.getFieldList().size());
    RelDataType flattenedType = DataTypeUtils.flatten(rowType, typeFactory);
    Assertions.assertEquals(2, flattenedType.getFieldList().size());
    List<String> flattenedNames = flattenedType.getFieldList().stream().map(x -> x.getName())
        .collect(Collectors.toList());
    Assertions.assertIterableEquals(Arrays.asList(new String[]{"FOO", "BAR"}),
        flattenedNames);
    String flattenedConnector = new ScriptImplementor.ConnectorImplementor("T1",
        flattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `T1` (`FOO` ANY ARRAY, "
        + "`BAR` ANY ARRAY) WITH ();", flattenedConnector,
        "Flattened connector should have simplified arrays");
  }
}

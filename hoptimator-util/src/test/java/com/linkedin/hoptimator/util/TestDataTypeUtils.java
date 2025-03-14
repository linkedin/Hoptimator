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
    builder3.add("DAY", typeFactory.createArrayType(typeFactory.createArrayType(typeFactory.createArrayType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), -1), -1), -1));
    RelDataType rowType = builder3.build();
    Assertions.assertEquals(4, rowType.getFieldList().size());
    RelDataType flattenedType = DataTypeUtils.flatten(rowType, typeFactory);
    Assertions.assertEquals(9, flattenedType.getFieldList().size());
    List<String> flattenedNames = flattenedType.getFieldList().stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
    System.out.println(flattenedNames);
    Assertions.assertIterableEquals(Arrays.asList("FOO", "FOO$QUX", "FOO$QIZ", "BAR", "BAR$BAZ", "CAR", "DAY",
            "DAY$__ARRTYPE__", "DAY$__ARRTYPE__$__ARRTYPE__"), flattenedNames);
    String flattenedConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        flattenedType, Collections.emptyMap()).sql();
    System.out.println(flattenedConnector);
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `S`.`T1` ("
            + "`FOO` ANY ARRAY, `FOO_QUX` VARCHAR, `FOO_QIZ` VARCHAR ARRAY, "
            + "`BAR` ANY ARRAY, `BAR_BAZ` VARCHAR, "
            + "`CAR` FLOAT ARRAY, "
            + "`DAY` ANY ARRAY, `DAY___ARRTYPE__` ANY ARRAY, `DAY___ARRTYPE_____ARRTYPE__` VARCHAR ARRAY) WITH ();",
        flattenedConnector, "Flattened connector should have simplified arrays");

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
            + "`CAR` FLOAT ARRAY, "
            + "`DAY` VARCHAR ARRAY ARRAY ARRAY) WITH ();", unflattenedConnector,
        "Flattened-unflattened connector should be correct");
  }

  @Test
  public void flattenUnflattenComplexMap() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder mapValue = new RelDataTypeFactory.Builder(typeFactory);
    mapValue.add("BAR", SqlTypeName.VARCHAR);
    mapValue.add("CAR", SqlTypeName.INTEGER);

    RelDataTypeFactory.Builder keyBuilder = new RelDataTypeFactory.Builder(typeFactory);
    RelDataTypeFactory.Builder valueBuilder = new RelDataTypeFactory.Builder(typeFactory);
    keyBuilder.add("QUX", SqlTypeName.VARCHAR);
    valueBuilder.add("QIZ", mapValue.build());

    RelDataTypeFactory.Builder mapBuilder = new RelDataTypeFactory.Builder(typeFactory);
    mapBuilder.add("FOO", typeFactory.createMapType(keyBuilder.build(), valueBuilder.build()));
    RelDataType rowType = mapBuilder.build();
    Assertions.assertEquals(1, rowType.getFieldList().size());
    RelDataType flattenedType = DataTypeUtils.flatten(rowType, typeFactory);
    Assertions.assertEquals(3, flattenedType.getFieldList().size());
    List<String> flattenedNames = flattenedType.getFieldList().stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
    Assertions.assertIterableEquals(Arrays.asList("FOO$__MAPKEYTYPE__", "FOO$__MAPVALUETYPE__$QIZ$BAR", "FOO$__MAPVALUETYPE__$QIZ$CAR"), flattenedNames);
    String flattenedConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        flattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `S`.`T1` ("
            + "`FOO___MAPKEYTYPE__` ROW(`QUX` VARCHAR), "
            + "`FOO___MAPVALUETYPE___QIZ_BAR` VARCHAR, "
            + "`FOO___MAPVALUETYPE___QIZ_CAR` INTEGER) WITH ();", flattenedConnector,
        "Flattened connector should have simplified map");

    RelDataType unflattenedType = DataTypeUtils.unflatten(flattenedType, typeFactory);
    RelOptUtil.eq("original", rowType, "flattened-unflattened", unflattenedType, Litmus.THROW);
    String originalConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        rowType, Collections.emptyMap()).sql();
    String unflattenedConnector = new ScriptImplementor.ConnectorImplementor("S", "T1",
        unflattenedType, Collections.emptyMap()).sql();
    Assertions.assertEquals(originalConnector, unflattenedConnector,
        "Flattening and unflattening data types should have no impact on connector");
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS `S`.`T1` ("
            + "`FOO` MAP< ROW(`QUX` VARCHAR), ROW(`QIZ` ROW(`BAR` VARCHAR, `CAR` INTEGER)) >) WITH ();", unflattenedConnector,
        "Flattened-unflattened connector should be correct");
  }
}

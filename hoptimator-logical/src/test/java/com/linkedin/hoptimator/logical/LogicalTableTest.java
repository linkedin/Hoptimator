package com.linkedin.hoptimator.logical;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;

import static org.assertj.core.api.Assertions.assertThat;


public class LogicalTableTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  public void flatSchemaConvertsBasicTypes() {
    String avroSchema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"FlatRecord\","
        + "\"namespace\":\"test\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\"},"
        + "  {\"name\":\"count\",\"type\":\"long\"},"
        + "  {\"name\":\"label\",\"type\":\"string\"},"
        + "  {\"name\":\"active\",\"type\":\"boolean\"}"
        + "]}";

    LogicalTable table = tableWithSchema(avroSchema);
    RelDataType rowType = table.getRowType(typeFactory);

    assertThat(rowType.isStruct()).isTrue();
    assertThat(rowType.getField("id", false, false)).isNotNull();
    assertThat(rowType.getField("count", false, false)).isNotNull();
    assertThat(rowType.getField("label", false, false)).isNotNull();
    assertThat(rowType.getField("active", false, false)).isNotNull();

    assertThat(rowType.getField("id", false, false).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.INTEGER);
    assertThat(rowType.getField("count", false, false).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.BIGINT);
    assertThat(rowType.getField("label", false, false).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.VARCHAR);
    assertThat(rowType.getField("active", false, false).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.BOOLEAN);
  }

  @Test
  public void nestedRecordFieldConvertsToStruct() {
    String avroSchema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Outer\","
        + "\"namespace\":\"test\","
        + "\"fields\":["
        + "  {\"name\":\"inner\",\"type\":{"
        + "    \"type\":\"record\","
        + "    \"name\":\"Inner\","
        + "    \"fields\":["
        + "      {\"name\":\"x\",\"type\":\"int\"}"
        + "    ]"
        + "  }}"
        + "]}";

    LogicalTable table = tableWithSchema(avroSchema);
    RelDataType rowType = table.getRowType(typeFactory);

    assertThat(rowType.isStruct()).isTrue();
    RelDataType innerType = rowType.getField("inner", false, false).getType();
    assertThat(innerType.isStruct()).isTrue();
    assertThat(innerType.getField("x", false, false)).isNotNull();
    assertThat(innerType.getField("x", false, false).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.INTEGER);
  }

  @Test
  public void nullableUnionFieldIsNullable() {
    String avroSchema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"NullableRecord\","
        + "\"namespace\":\"test\","
        + "\"fields\":["
        + "  {\"name\":\"maybeString\",\"type\":[\"null\",\"string\"],\"default\":null}"
        + "]}";

    LogicalTable table = tableWithSchema(avroSchema);
    RelDataType rowType = table.getRowType(typeFactory);

    RelDataType fieldType = rowType.getField("maybeString", false, false).getType();
    assertThat(fieldType.isNullable()).isTrue();
    assertThat(fieldType.getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  public void arrayFieldConvertsToArrayType() {
    String avroSchema = "{"
        + "\"type\":\"record\","
        + "\"name\":\"ArrayRecord\","
        + "\"namespace\":\"test\","
        + "\"fields\":["
        + "  {\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
        + "]}";

    LogicalTable table = tableWithSchema(avroSchema);
    RelDataType rowType = table.getRowType(typeFactory);

    RelDataType fieldType = rowType.getField("tags", false, false).getType();
    assertThat(fieldType.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    assertThat(fieldType.getComponentType()).isNotNull();
    assertThat(fieldType.getComponentType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  public void emptyAvroSchemaReturnsEmptyStruct() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    // avroSchema is null
    LogicalTable table = new LogicalTable("empty", spec);
    RelDataType rowType = table.getRowType(typeFactory);

    assertThat(rowType.isStruct()).isTrue();
    assertThat(rowType.getFieldCount()).isEqualTo(0);
  }

  // --- helpers ---

  private LogicalTable tableWithSchema(String avroSchemaJson) {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.setAvroSchema(avroSchemaJson);
    return new LogicalTable("test", spec);
  }
}

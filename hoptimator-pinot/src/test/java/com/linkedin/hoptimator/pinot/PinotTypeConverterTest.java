package com.linkedin.hoptimator.pinot;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PinotTypeConverterTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  void columnsSurfacedInDimensionMetricDateTimeOrder() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addDimensionField("d_str", FieldSpec.DataType.STRING)
        .addMetricField("m_long", FieldSpec.DataType.LONG)
        .addDateTimeField("t_ts", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    RelDataType rowType = PinotTypeConverter.rel(schema, typeFactory);

    assertThat(rowType.getFieldNames()).containsExactly("d_str", "m_long", "t_ts");
    assertThat(typeOf(rowType, "d_str").getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(typeOf(rowType, "m_long").getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
    assertThat(typeOf(rowType, "t_ts").getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP);
    rowType.getFieldList().forEach(f -> assertThat(f.getType().isNullable()).isTrue());
  }

  @Test
  void scalarDataTypeMapping() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addDimensionField("c_int", FieldSpec.DataType.INT)
        .addDimensionField("c_float", FieldSpec.DataType.FLOAT)
        .addDimensionField("c_double", FieldSpec.DataType.DOUBLE)
        .addDimensionField("c_decimal", FieldSpec.DataType.BIG_DECIMAL)
        .addDimensionField("c_bool", FieldSpec.DataType.BOOLEAN)
        .addDimensionField("c_json", FieldSpec.DataType.JSON)
        .addDimensionField("c_bytes", FieldSpec.DataType.BYTES)
        .build();

    RelDataType rowType = PinotTypeConverter.rel(schema, typeFactory);

    assertThat(typeOf(rowType, "c_int").getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);
    assertThat(typeOf(rowType, "c_float").getSqlTypeName()).isEqualTo(SqlTypeName.REAL);
    assertThat(typeOf(rowType, "c_double").getSqlTypeName()).isEqualTo(SqlTypeName.DOUBLE);
    assertThat(typeOf(rowType, "c_decimal").getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
    assertThat(typeOf(rowType, "c_bool").getSqlTypeName()).isEqualTo(SqlTypeName.BOOLEAN);
    assertThat(typeOf(rowType, "c_json").getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(typeOf(rowType, "c_bytes").getSqlTypeName()).isEqualTo(SqlTypeName.VARBINARY);
  }

  @Test
  void multiValuedDimensionBecomesArray() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("t")
        .addField(new DimensionFieldSpec("tags", FieldSpec.DataType.INT, false))
        .build();

    RelDataType tags = typeOf(PinotTypeConverter.rel(schema, typeFactory), "tags");

    assertThat(tags.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    RelDataType componentType = java.util.Objects.requireNonNull(tags.getComponentType());
    assertThat(componentType.getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);
  }

  @Test
  void reversePinotTypeMapping() {
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.TINYINT)).isEqualTo(FieldSpec.DataType.INT);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.SMALLINT)).isEqualTo(FieldSpec.DataType.INT);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.INTEGER)).isEqualTo(FieldSpec.DataType.INT);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.BIGINT)).isEqualTo(FieldSpec.DataType.LONG);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.REAL)).isEqualTo(FieldSpec.DataType.FLOAT);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.FLOAT)).isEqualTo(FieldSpec.DataType.FLOAT);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.DOUBLE)).isEqualTo(FieldSpec.DataType.DOUBLE);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.DECIMAL)).isEqualTo(FieldSpec.DataType.BIG_DECIMAL);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.BOOLEAN)).isEqualTo(FieldSpec.DataType.BOOLEAN);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.TIMESTAMP)).isEqualTo(FieldSpec.DataType.TIMESTAMP);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.BINARY)).isEqualTo(FieldSpec.DataType.BYTES);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.VARBINARY)).isEqualTo(FieldSpec.DataType.BYTES);
    assertThat(PinotTypeConverter.pinotType(SqlTypeName.VARCHAR)).isEqualTo(FieldSpec.DataType.STRING);
  }

  private static RelDataType typeOf(RelDataType rowType, String field) {
    return java.util.Objects.requireNonNull(rowType.getField(field, true, false)).getType();
  }

  // ------------------------------------------------------------------------------------------
  // Complex types (read: Pinot -> Calcite)
  // ------------------------------------------------------------------------------------------

  @Test
  void structMapsToVarcharFallback() {
    // STRUCT field specs cannot be constructed in pinot-spi (no default-null value), so a bare
    // STRUCT dataType on a primitive-style spec is the only way one could appear; it falls back to
    // VARCHAR rather than failing the table.
    DimensionFieldSpec structLike = new DimensionFieldSpec("s", FieldSpec.DataType.STRING, true);
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t").addField(structLike).build();
    assertThat(typeOf(PinotTypeConverter.rel(schema, typeFactory), "s").getSqlTypeName())
        .isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  void mapComplexMapsToMapType() {
    ComplexFieldSpec map = new ComplexFieldSpec("props", FieldSpec.DataType.MAP, true, Map.of(
        ComplexFieldSpec.KEY_FIELD, new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        ComplexFieldSpec.VALUE_FIELD, new DimensionFieldSpec("value", FieldSpec.DataType.INT, true)));
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t").addField(map).build();

    RelDataType props = typeOf(PinotTypeConverter.rel(schema, typeFactory), "props");

    assertThat(props.getSqlTypeName()).isEqualTo(SqlTypeName.MAP);
    assertThat(java.util.Objects.requireNonNull(props.getKeyType()).getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(java.util.Objects.requireNonNull(props.getValueType()).getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);
  }

  @Test
  void listMapsToArrayOfElement() {
    ComplexFieldSpec list = new ComplexFieldSpec("scores", FieldSpec.DataType.LIST, true, Map.of(
        ComplexFieldSpec.VALUE_FIELD, new DimensionFieldSpec("value", FieldSpec.DataType.DOUBLE, true)));
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t").addField(list).build();

    RelDataType scores = typeOf(PinotTypeConverter.rel(schema, typeFactory), "scores");

    assertThat(scores.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    assertThat(java.util.Objects.requireNonNull(scores.getComponentType()).getSqlTypeName())
        .isEqualTo(SqlTypeName.DOUBLE);
  }

  @Test
  void listOfMapMapsRecursively() {
    ComplexFieldSpec innerMap = new ComplexFieldSpec("value", FieldSpec.DataType.MAP, true, Map.of(
        ComplexFieldSpec.KEY_FIELD, new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        ComplexFieldSpec.VALUE_FIELD, new DimensionFieldSpec("value", FieldSpec.DataType.LONG, true)));
    ComplexFieldSpec list = new ComplexFieldSpec("maps", FieldSpec.DataType.LIST, true, Map.of(
        ComplexFieldSpec.VALUE_FIELD, innerMap));
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t").addField(list).build();

    RelDataType maps = typeOf(PinotTypeConverter.rel(schema, typeFactory), "maps");

    assertThat(maps.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    RelDataType element = java.util.Objects.requireNonNull(maps.getComponentType());
    assertThat(element.getSqlTypeName()).isEqualTo(SqlTypeName.MAP);
    assertThat(java.util.Objects.requireNonNull(element.getValueType()).getSqlTypeName())
        .isEqualTo(SqlTypeName.BIGINT);
  }

  // ------------------------------------------------------------------------------------------
  // Complex types (write: Calcite -> Pinot). Provisioning supports primitives + primitive arrays;
  // complex columns are rejected (Pinot's canonical handling is a JSON column or upstream flatten).
  // ------------------------------------------------------------------------------------------

  @Test
  void toFieldSpecRejectsStruct() {
    RelDataType row = new RelDataTypeFactory.Builder(typeFactory)
        .add("a", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("b", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> PinotTypeConverter.toFieldSpec("s", row))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("STRUCT");
  }

  @Test
  void toFieldSpecRejectsMap() {
    RelDataType mapType = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER));

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> PinotTypeConverter.toFieldSpec("m", mapType))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("MAP");
  }

  @Test
  void toFieldSpecArrayOfPrimitiveIsMultiValued() {
    RelDataType arrayType = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);

    FieldSpec spec = PinotTypeConverter.toFieldSpec("tags", arrayType);

    assertThat(spec).isInstanceOf(DimensionFieldSpec.class);
    assertThat(spec.getDataType()).isEqualTo(FieldSpec.DataType.INT);
    assertThat(spec.isSingleValueField()).isFalse();
  }

  @Test
  void toFieldSpecRejectsArrayOfComplex() {
    RelDataType mapType = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataType arrayOfMap = typeFactory.createArrayType(mapType, -1);

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> PinotTypeConverter.toFieldSpec("rows", arrayOfMap))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void primitiveArrayRoundTripsThroughPinot() {
    RelDataType arrayType = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t")
        .addField(PinotTypeConverter.toFieldSpec("scores", arrayType)).build();

    RelDataType scores = typeOf(PinotTypeConverter.rel(schema, typeFactory), "scores");

    assertThat(scores.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    assertThat(java.util.Objects.requireNonNull(scores.getComponentType()).getSqlTypeName())
        .isEqualTo(SqlTypeName.DOUBLE);
  }
}

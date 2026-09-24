package com.linkedin.hoptimator.pinot;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PinotTableTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  void getRowTypeMapsSchema() throws Exception {
    PinotClient client = mock(PinotClient.class);
    Schema schema = new Schema.SchemaBuilder().setSchemaName("myTable")
        .addDimensionField("userId", FieldSpec.DataType.STRING).build();
    when(client.getSchema("myTable")).thenReturn(schema);

    RelDataType rowType = new PinotTable("myTable", client).getRowType(typeFactory);

    assertThat(rowType.getFieldCount()).isEqualTo(1);
    assertThat(rowType.getFieldList().get(0).getName()).isEqualTo("userId");
    assertThat(rowType.getFieldList().get(0).getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  void getRowTypeFlattensComplexColumnsForJdbcLayer() throws Exception {
    // A MAP column must surface flattened ($-delimited) like the other drivers' JDBC row types,
    // not as a raw Calcite MAP.
    PinotClient client = mock(PinotClient.class);
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t")
        .addDimensionField("id", FieldSpec.DataType.STRING)
        .addField(new org.apache.pinot.spi.data.ComplexFieldSpec(
            "props", FieldSpec.DataType.MAP, true, java.util.Map.of(
                org.apache.pinot.spi.data.ComplexFieldSpec.KEY_FIELD,
                new org.apache.pinot.spi.data.DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
                org.apache.pinot.spi.data.ComplexFieldSpec.VALUE_FIELD,
                new org.apache.pinot.spi.data.DimensionFieldSpec("value", FieldSpec.DataType.INT, true))))
        .build();
    when(client.getSchema("t")).thenReturn(schema);

    RelDataType rowType = new PinotTable("t", client).getRowType(typeFactory);

    // The MAP is flattened away — no field is a raw MAP; the map key/value surface as scalar columns.
    assertThat(rowType.getFieldList()).noneMatch(f -> f.getType().getSqlTypeName() == SqlTypeName.MAP);
    assertThat(rowType.getFieldNames()).contains("id");
    assertThat(rowType.getFieldNames()).anyMatch(n -> n.startsWith("props$"));
  }

  @Test
  void getRowTypeReturnsUnknownWhenSchemaMissing() throws Exception {
    PinotClient client = mock(PinotClient.class);
    when(client.getSchema("bare")).thenReturn(null);

    RelDataType rowType = new PinotTable("bare", client).getRowType(typeFactory);

    assertThat(rowType).isNotNull();
    assertThat(rowType.isStruct()).isFalse();
  }

  @Test
  void getRowTypeReturnsUnknownOnFailure() throws Exception {
    PinotClient client = mock(PinotClient.class);
    when(client.getSchema("bad")).thenThrow(new java.io.IOException("controller down"));

    // Per-table schema failures must not fail catalog resolution.
    assertThat(new PinotTable("bad", client).getRowType(typeFactory)).isNotNull();
  }
}

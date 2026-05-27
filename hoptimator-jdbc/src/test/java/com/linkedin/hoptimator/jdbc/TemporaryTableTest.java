package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;


/**
 * Unit tests for {@link TemporaryTable}.
 */
class TemporaryTableTest {

  private static RelDataTypeFactory factory() {
    return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  private static RelDataType buildRowType(RelDataTypeFactory f) {
    return f.builder()
        .add("ID", SqlTypeName.INTEGER)
        .add("NAME", SqlTypeName.VARCHAR)
        .build();
  }

  @Test
  void simpleConstructor() {
    RelDataTypeFactory f = factory();
    RelDataType rowType = buildRowType(f);
    TemporaryTable table = new TemporaryTable(rowType, "mydb");
    assertEquals(rowType, table.getRowType(f));
    assertEquals("mydb", table.databaseName());
    assertSame(NullInitializerExpressionFactory.INSTANCE, table.unwrap(InitializerExpressionFactory.class));
  }

  @Test
  void unwrapReturnsNullForUnknownType() {
    RelDataTypeFactory f = factory();
    TemporaryTable table = new TemporaryTable(buildRowType(f), "mydb");
    assertNull(table.unwrap(String.class));
  }

  @Test
  void isModifiableTable() {
    RelDataTypeFactory f = factory();
    TemporaryTable table = new TemporaryTable(buildRowType(f), "mydb");
    // TemporaryTable extends ArrayTable which implements ModifiableTable.
    // Verify by assigning to ModifiableTable and calling a ModifiableTable method.
    ModifiableTable modifiable = table;
    assertNotNull(modifiable.getModifiableCollection());
  }
}

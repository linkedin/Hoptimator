package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class MaterializedViewTableTest {

  @Mock
  private RelOptTable.ToRelContext mockToRelContext;

  @Mock
  private RelOptTable mockRelOptTable;

  @Mock
  private RelNode mockRelNode;

  private ViewTable viewTable;
  private MaterializedViewTable materializedViewTable;

  @BeforeEach
  void setUp() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("ID", SqlTypeName.INTEGER)
        .add("NAME", SqlTypeName.VARCHAR)
        .build();
    RelProtoDataType protoType = RelDataTypeImpl.proto(rowType);
    List<String> viewPath = Arrays.asList("db", "schema");
    viewTable = new ViewTable(Object.class, protoType, "SELECT 1", List.of("root"), viewPath);
    materializedViewTable = new MaterializedViewTable(viewTable);
  }

  @Test
  void testViewTableReturnsOriginal() {
    assertSame(viewTable, materializedViewTable.viewTable());
  }

  @Test
  void testViewSqlReturnsQueryString() {
    assertEquals("SELECT 1", materializedViewTable.viewSql());
  }

  @Test
  void testViewPathReturnsPath() {
    assertEquals(Arrays.asList("db", "schema"), materializedViewTable.viewPath());
  }

  @Test
  void testGetJdbcTableTypeReturnsMaterializedView() {
    assertEquals(Schema.TableType.MATERIALIZED_VIEW, materializedViewTable.getJdbcTableType());
  }

  @Test
  void testGetRowTypeDelegatesToViewTable() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataType rowType = materializedViewTable.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertEquals("ID", rowType.getFieldNames().get(0));
    assertEquals("NAME", rowType.getFieldNames().get(1));
  }

  @Test
  void testToRelDelegatesToViewTable() {
    ViewTable mockViewTable = mock(ViewTable.class);
    when(mockViewTable.toRel(mockToRelContext, mockRelOptTable)).thenReturn(mockRelNode);
    MaterializedViewTable table = new MaterializedViewTable(mockViewTable);

    RelNode result = table.toRel(mockToRelContext, mockRelOptTable);

    assertSame(mockRelNode, result);
  }
}

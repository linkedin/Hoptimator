package com.linkedin.hoptimator.util.planner;

import java.sql.Connection;
import java.util.Collections;

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class HoptimatorJdbcTableTest {

  @Mock
  private JdbcTable mockJdbcTable;

  @Mock
  private Connection mockConnection;

  @Mock
  private Expression mockExpression;

  private HoptimatorJdbcConvention convention;
  private HoptimatorJdbcTable table;

  @BeforeEach
  void setUp() {
    convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "db", Collections.emptyList(), mockConnection);
    table = new HoptimatorJdbcTable(mockJdbcTable, convention);
  }

  @Test
  void testJdbcTableReturnsWrapped() {
    assertSame(mockJdbcTable, table.jdbcTable());
  }

  @Test
  void testGetModifiableCollectionReturnsNull() {
    assertNull(table.getModifiableCollection());
  }

  @Test
  void testGetRowTypeUnflattens() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType simpleType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    when(mockJdbcTable.getRowType(any(RelDataTypeFactory.class))).thenReturn(simpleType);

    RelDataType result = table.getRowType(typeFactory);

    assertSame(simpleType.getFieldList().get(0).getName(),
        result.getFieldList().get(0).getName());
  }

  @Test
  void testToModificationRelDelegatesToJdbcTable() {
    RelOptCluster mockCluster = mock(RelOptCluster.class);
    RelOptTable mockRelOptTable = mock(RelOptTable.class);
    Prepare.CatalogReader mockCatalogReader = mock(Prepare.CatalogReader.class);
    RelNode mockInput = mock(RelNode.class);
    TableModify mockTableModify = mock(TableModify.class);

    when(mockJdbcTable.toModificationRel(any(), any(), any(), any(), any(), any(), any(), any(boolean.class)))
        .thenReturn(mockTableModify);

    TableModify result = table.toModificationRel(mockCluster, mockRelOptTable, mockCatalogReader,
        mockInput, TableModify.Operation.INSERT, null, null, false);

    assertSame(mockTableModify, result);
    verify(mockJdbcTable).toModificationRel(mockCluster, mockRelOptTable, mockCatalogReader,
        mockInput, TableModify.Operation.INSERT, null, null, false);
  }

  @Test
  void testAsQueryableDelegatesToJdbcTable() {
    QueryProvider mockProvider = mock(QueryProvider.class);
    SchemaPlus mockSchema = mock(SchemaPlus.class);
    Queryable<?> mockQueryable = mock(Queryable.class);

    when(mockJdbcTable.asQueryable(any(), any(), any())).thenReturn((Queryable) mockQueryable);

    Queryable<Object> result = table.asQueryable(mockProvider, mockSchema, "tableName");

    assertSame(mockQueryable, result);
    verify(mockJdbcTable).asQueryable(mockProvider, mockSchema, "tableName");
  }
}

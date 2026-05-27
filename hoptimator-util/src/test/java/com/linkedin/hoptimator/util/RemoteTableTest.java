package com.linkedin.hoptimator.util;

import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class RemoteTableTest {

  @Mock
  private Api<String> mockApi;

  @Test
  void testRowsReturnsRemoteRowList() throws SQLException {
    when(mockApi.list()).thenReturn(Arrays.asList("a", "b"));

    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);
    Collection<TestRow> rows = table.rows();

    assertNotNull(rows);
    assertEquals(2, rows.size());
  }

  @Test
  void testApiReturnsProvidedApi() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);

    assertSame(mockApi, table.api());
  }

  @Test
  void testGetElementTypeReturnsRowClass() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);

    assertEquals(TestRow.class, table.getElementType());
  }

  @Test
  void testFromRowThrowsUnsupportedByDefault() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);

    assertThrows(UnsupportedOperationException.class, () -> table.fromRow(new TestRow("x")));
  }

  @Test
  void testGetModifiableCollectionReturnsSameAsRows() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);

    assertSame(table.rows(), table.getModifiableCollection());
  }

  @Test
  void testGetRowTypeReturnsCopiedType() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(1, rowType.getFieldCount());
  }

  @Test
  void testAsQueryableReturnsQueryable() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);

    Queryable<TestRow> queryable = table.asQueryable(null, null, "test");

    assertNotNull(queryable);
  }

  @Test
  void testGetExpressionReturnsExpression() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("testTable", table);

    Expression expression = table.getExpression(rootSchema, "testTable", TestRow.class);

    assertNotNull(expression);
  }

  @Test
  void testToModificationRelReturnsLogicalTableModify() {
    RemoteTable<String, TestRow> table = new TestRemoteTable(mockApi);
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    RelOptTable mockTable = mock(RelOptTable.class);
    Prepare.CatalogReader mockCatalogReader = mock(Prepare.CatalogReader.class);
    RelNode mockInput = mock(RelNode.class);

    TableModify result = table.toModificationRel(cluster, mockTable, mockCatalogReader,
        mockInput, TableModify.Operation.INSERT, null, null, false);

    assertNotNull(result);
    assertInstanceOf(LogicalTableModify.class, result);
  }

  public static class TestRow {
    public String value;
    TestRow() {
      this.value = null;
    }
    TestRow(String value) {
      this.value = value;
    }
  }

  private static class TestRemoteTable extends RemoteTable<String, TestRow> {
    TestRemoteTable(Api<String> api) {
      super(api, TestRow.class);
    }

    @Override
    public TestRow toRow(String s) {
      return new TestRow(s);
    }
  }
}

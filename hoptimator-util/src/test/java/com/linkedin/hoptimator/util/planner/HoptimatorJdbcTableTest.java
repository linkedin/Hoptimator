package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.avro.AvroSchemaSource;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
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

  private HoptimatorJdbcTable table;

  @BeforeEach
  void setUp() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
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
  void valueSchemaReturnsNullWhenNoUpstreamTable() {
    HoptimatorJdbcTable tableWithNullUpstream = new HoptimatorJdbcTable(mockJdbcTable,
        new HoptimatorJdbcConvention(AnsiSqlDialect.DEFAULT, mockExpression, "db",
            Collections.emptyList(), mockConnection)) {
      @Override
      Table upstreamTable() {
        return null;
      }
    };
    assertNull(tableWithNullUpstream.valueSchema());
    assertNull(tableWithNullUpstream.keySchema());
  }

  @Test
  void valueSchemaReturnsNullWhenUpstreamDoesNotImplementSource() {
    Table upstream = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        throw new UnsupportedOperationException();
      }
    };
    HoptimatorJdbcTable wrapper = new HoptimatorJdbcTable(mockJdbcTable,
        new HoptimatorJdbcConvention(AnsiSqlDialect.DEFAULT, mockExpression, "db",
            Collections.emptyList(), mockConnection)) {
      @Override
      Table upstreamTable() {
        return upstream;
      }
    };
    assertNull(wrapper.valueSchema());
    assertNull(wrapper.keySchema());
  }

  @Test
  void valueAndKeySchemaDelegateToUpstreamSource() {
    Schema expectedValue = SchemaBuilder.record("Foo").namespace("com.linkedin.bar").fields()
        .requiredString("v").endRecord();
    Schema expectedKey = SchemaBuilder.record("FooKey").namespace("com.linkedin.keyns").fields()
        .requiredString("id").endRecord();
    Table upstream = new SourceTable(expectedValue, expectedKey);
    HoptimatorJdbcTable wrapper = new HoptimatorJdbcTable(mockJdbcTable,
        new HoptimatorJdbcConvention(AnsiSqlDialect.DEFAULT, mockExpression, "db",
            Collections.emptyList(), mockConnection)) {
      @Override
      Table upstreamTable() {
        return upstream;
      }
    };
    assertSame(expectedValue, wrapper.valueSchema());
    assertSame(expectedKey, wrapper.keySchema());
  }

  @Test
  void keySchemaReturnsNullWhenUpstreamHasNoKey() {
    Schema valueOnly = SchemaBuilder.record("Foo").namespace("ns").fields()
        .requiredString("v").endRecord();
    Table upstream = new SourceTable(valueOnly, null);
    HoptimatorJdbcTable wrapper = new HoptimatorJdbcTable(mockJdbcTable,
        new HoptimatorJdbcConvention(AnsiSqlDialect.DEFAULT, mockExpression, "db",
            Collections.emptyList(), mockConnection)) {
      @Override
      Table upstreamTable() {
        return upstream;
      }
    };
    assertSame(valueOnly, wrapper.valueSchema());
    assertNull(wrapper.keySchema(), "upstream with no key should propagate null");
  }

  private static final class SourceTable extends AbstractTable implements AvroSchemaSource {
    private final Schema value;
    private final Schema key;

    SourceTable(Schema value, Schema key) {
      this.value = value;
      this.key = key;
    }

    @Override
    public Schema valueSchema() {
      return value;
    }

    @Override
    public Schema keySchema() {
      return key;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  void testAsQueryableDelegatesToJdbcTable() {
    QueryProvider mockProvider = mock(QueryProvider.class);
    SchemaPlus mockSchema = mock(SchemaPlus.class);
    Queryable<?> mockQueryable = mock(Queryable.class);

    doReturn(mockQueryable).when(mockJdbcTable).asQueryable(any(), any(), any());

    Queryable<Object> result = table.asQueryable(mockProvider, mockSchema, "tableName");

    assertSame(mockQueryable, result);
    verify(mockJdbcTable).asQueryable(mockProvider, mockSchema, "tableName");
  }
}

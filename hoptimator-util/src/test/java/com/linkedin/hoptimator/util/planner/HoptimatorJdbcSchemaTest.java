package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Engine;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class HoptimatorJdbcSchemaTest {

  @Mock
  private DataSource mockDataSource;

  @Mock
  private Connection mockConnection;

  @Mock
  private Expression mockExpression;

  @Mock
  private Engine mockEngine;

  @Test
  void testDatabaseNameReturnsConstructorValue() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    assertEquals("myDb", schema.databaseName());
  }

  @Test
  void testEnginesReturnsProvidedList() {
    List<Engine> engines = Collections.singletonList(mockEngine);
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", engines, mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, engines);

    assertEquals(1, schema.engines().size());
    assertSame(mockEngine, schema.engines().get(0));
  }

  @Test
  void testSnapshotReturnsSelf() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Schema snapshot = schema.snapshot(null);

    assertSame(schema, snapshot);
  }

  @Test
  void testTablesReturnsLookup() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<Table> tables = schema.tables();

    assertNotNull(tables);
  }

  @Test
  void testTablesLookupReturnsNullForMissingTable() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<Table> tables = schema.tables();
    Table result = tables.get("nonexistent_table");

    assertNull(result);
  }

  @Test
  void testStaticCreateReturnsSchema() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mySchema", new AbstractSchema());

    HoptimatorJdbcSchema schema = HoptimatorJdbcSchema.create(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        rootSchema, AnsiSqlDialect.DEFAULT, Collections.emptyList(), mockConnection);

    assertNotNull(schema);
    assertEquals("myDb", schema.databaseName());
  }

  @Test
  void testTablesReturnsCachedLookup() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<Table> tables1 = schema.tables();
    Lookup<Table> tables2 = schema.tables();

    assertSame(tables1, tables2);
  }

  @Test
  void testTablesGetNamesReturnsEmptyForNoTables() throws Exception {
    Connection dsConnection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(dsConnection);
    when(dsConnection.getMetaData()).thenReturn(metaData);
    when(metaData.getTables(
        anyString(),
        anyString(),
        anyString(),
        isNull())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<Table> tables = schema.tables();
    Set<String> names = tables.getNames(LikePattern.any());

    assertNotNull(names);
    assertTrue(names.isEmpty());
  }
}

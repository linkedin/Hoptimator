package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Engine;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class HoptimatorJdbcCatalogSchemaTest {

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
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    assertEquals("myDb", schema.databaseName());
  }

  @Test
  void testCatalogReturnsConstructorValue() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    assertEquals("myCatalog", schema.catalog());
  }

  @Test
  void testEnginesReturnsProvidedList() {
    List<Engine> engines = Collections.singletonList(mockEngine);
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", engines, mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, engines);

    assertEquals(1, schema.engines().size());
    assertSame(mockEngine, schema.engines().get(0));
  }

  @Test
  void testSnapshotReturnsSelf() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Schema snapshot = schema.snapshot(null);

    assertSame(schema, snapshot);
  }

  @Test
  void testCreateSchemaReturnsJdbcSchema() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    HoptimatorJdbcSchema subSchema = schema.createSchema("mySchema");

    assertNotNull(subSchema);
    assertEquals("myDb", subSchema.databaseName());
  }

  @Test
  void testSubSchemasLookupReturnsNonNull() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    assertNotNull(schema.subSchemas());
  }

  @Test
  void testStaticCreateReturnsSchema() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mySchema", new AbstractSchema());

    HoptimatorJdbcCatalogSchema schema = HoptimatorJdbcCatalogSchema.create(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        rootSchema, AnsiSqlDialect.DEFAULT, Collections.emptyList(), mockConnection);

    assertNotNull(schema);
    assertEquals("myDb", schema.databaseName());
    assertEquals("myCatalog", schema.catalog());
  }

  @Test
  void testStaticCreateWithEngines() {
    List<Engine> engines = Collections.singletonList(mockEngine);
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mySchema", new AbstractSchema());

    HoptimatorJdbcCatalogSchema schema = HoptimatorJdbcCatalogSchema.create(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        rootSchema, AnsiSqlDialect.DEFAULT, engines, mockConnection);

    assertNotNull(schema);
    assertEquals(1, schema.engines().size());
  }

  @Test
  void testSubSchemasGetReturnsSchemaWhenFound() throws SQLException {
    Connection dsConn = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(dsConn);
    when(dsConn.getMetaData()).thenReturn(metaData);
    when(metaData.getSchemas(eq("myCatalog"), eq("mySchema"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn("mySchema");

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<? extends Schema> subSchemas = schema.subSchemas();
    Schema result = subSchemas.get("mySchema");

    assertNotNull(result);
  }

  @Test
  void testSubSchemasGetReturnsNullWhenNotFound() throws SQLException {
    Connection dsConn = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(dsConn);
    when(dsConn.getMetaData()).thenReturn(metaData);
    when(metaData.getSchemas(eq("myCatalog"), eq("missing"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<? extends Schema> subSchemas = schema.subSchemas();
    Schema result = subSchemas.get("missing");

    assertNull(result);
  }

  @Test
  void testSubSchemasGetNamesReturnsSchemaNames() throws SQLException {
    Connection dsConn = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(dsConn);
    when(dsConn.getMetaData()).thenReturn(metaData);
    when(metaData.getSchemas(eq("myCatalog"), eq("%"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString(1)).thenReturn("schema1", "schema2");

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<? extends Schema> subSchemas = schema.subSchemas();
    Set<String> names = subSchemas.getNames(LikePattern.any());

    assertNotNull(names);
    assertTrue(names.size() >= 2);
  }

  @Test
  void testSubSchemasGetThrowsOnSqlException() throws SQLException {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("connection error"));

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<? extends Schema> subSchemas = schema.subSchemas();
    assertThrows(RuntimeException.class, () -> subSchemas.get("any"));
  }

  @Test
  void testSubSchemasGetNamesThrowsOnSqlException() throws SQLException {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("connection error"));

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcCatalogSchema schema = new HoptimatorJdbcCatalogSchema(
        "myDb", "myCatalog", mockDataSource, AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<? extends Schema> subSchemas = schema.subSchemas();
    assertThrows(RuntimeException.class, () -> subSchemas.getNames(LikePattern.any()));
  }
}

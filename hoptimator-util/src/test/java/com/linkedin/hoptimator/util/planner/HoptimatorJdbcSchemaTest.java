package com.linkedin.hoptimator.util.planner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.linkedin.hoptimator.Engine;
import org.apache.calcite.jdbc.CalciteConnection;
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(
    value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Tests stub DataSource.getConnection() / Connection.unwrap() on Mockito mocks. "
        + "The returned Connection is a mock — not a real JDBC resource — but SpotBugs can't tell "
        + "the difference from the bytecode signature.")
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

  // If getNames() returns empty set, the non-empty result below would not be found.
  @Test
  void testTablesGetNamesReturnsNonEmptySetWhenTablesExist() throws Exception {
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
    // Two rows returned — JdbcSchema.metaDataMapper reads getString(1)=catalog, getString(2)=schema,
    // getString(3)=tableName, getString(4)=tableType. Provide all values Mockito strict mode requires.
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString(1)).thenReturn("myCatalog", "myCatalog");
    when(resultSet.getString(2)).thenReturn("mySchema", "mySchema");
    when(resultSet.getString(3)).thenReturn("TABLE_A", "TABLE_B");
    when(resultSet.getString(4)).thenReturn("TABLE", "TABLE");

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    Lookup<Table> tables = schema.tables();
    Set<String> names = tables.getNames(LikePattern.any());

    assertNotNull(names);
    assertFalse(names.isEmpty(), "getNames() must return a non-empty set when tables exist");
    assertTrue(names.contains("TABLE_A") || names.size() >= 1,
        "at least one table name must appear in getNames() result");
  }

  /** Marker-tagged schema standing in for {@code LogicalTableSchema} in tests. */
  private static final class MarkerSchema extends AbstractSchema implements LogicalSchemaMarker {
  }

  private HoptimatorJdbcSchema schemaWithDownstream(String catalog, String schema, SchemaPlus downstreamRoot)
      throws SQLException {
    Connection dsConnection = mock(Connection.class);
    CalciteConnection calciteConnection = mock(CalciteConnection.class);
    when(mockDataSource.getConnection()).thenReturn(dsConnection);
    when(dsConnection.unwrap(CalciteConnection.class)).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(downstreamRoot);

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    return new HoptimatorJdbcSchema(
        "myDb", catalog, schema, mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());
  }

  // 2-level path: a logical driver registers its marker schema at root.<schema>.
  @Test
  void testIsLogicalReturnsTrueForTwoLevelDownstreamWithMarker() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);
    downstreamRoot.add("LOGICAL", new MarkerSchema());

    HoptimatorJdbcSchema schema = schemaWithDownstream(null, "LOGICAL", downstreamRoot);

    assertTrue(schema.isLogical());
  }

  // 2-level path: downstream has the named schema but it does not implement the marker.
  // SchemaPlus.unwrap() throws ClassCastException; detectLogical catches and returns false.
  @Test
  void testIsLogicalReturnsFalseForTwoLevelDownstreamWithoutMarker() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);
    downstreamRoot.add("REGULAR", new AbstractSchema());

    HoptimatorJdbcSchema schema = schemaWithDownstream(null, "REGULAR", downstreamRoot);

    assertFalse(schema.isLogical());
  }

  // 2-level path: downstream has no sub-schema matching the configured schema name.
  @Test
  void testIsLogicalReturnsFalseForTwoLevelDownstreamWithMissingSchema() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);

    HoptimatorJdbcSchema schema = schemaWithDownstream(null, "MISSING", downstreamRoot);

    assertFalse(schema.isLogical());
  }

  // 3-level path: marker registered at root.<catalog>.<schema>.
  @Test
  void testIsLogicalReturnsTrueForThreeLevelDownstreamWithMarker() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);
    SchemaPlus catalogSub = downstreamRoot.add("MYCATALOG", new AbstractSchema());
    catalogSub.add("MYSCHEMA", new MarkerSchema());

    HoptimatorJdbcSchema schema = schemaWithDownstream("MYCATALOG", "MYSCHEMA", downstreamRoot);

    assertTrue(schema.isLogical());
  }

  // 3-level path: catalog exists, schema under catalog does not implement the marker (typical MySQL/etc.).
  @Test
  void testIsLogicalReturnsFalseForThreeLevelDownstreamWithoutMarker() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);
    SchemaPlus catalogSub = downstreamRoot.add("MYCATALOG", new AbstractSchema());
    catalogSub.add("MYSCHEMA", new AbstractSchema());

    HoptimatorJdbcSchema schema = schemaWithDownstream("MYCATALOG", "MYSCHEMA", downstreamRoot);

    assertFalse(schema.isLogical());
  }

  // 3-level path: configured catalog isn't present in the downstream root.
  @Test
  void testIsLogicalReturnsFalseForThreeLevelDownstreamWithMissingCatalog() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);

    HoptimatorJdbcSchema schema = schemaWithDownstream("MISSING", "MYSCHEMA", downstreamRoot);

    assertFalse(schema.isLogical());
  }

  // 3-level path: catalog exists but the named schema underneath it doesn't.
  @Test
  void testIsLogicalReturnsFalseForThreeLevelDownstreamWithMissingSchemaUnderCatalog() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);
    downstreamRoot.add("MYCATALOG", new AbstractSchema());

    HoptimatorJdbcSchema schema = schemaWithDownstream("MYCATALOG", "MISSING", downstreamRoot);

    assertFalse(schema.isLogical());
  }

  // Non-Calcite downstream (e.g. real MySQL) — unwrap(CalciteConnection.class) throws and we fall through to false.
  @Test
  void testIsLogicalReturnsFalseWhenDownstreamIsNotCalcite() throws Exception {
    Connection dsConnection = mock(Connection.class);
    when(mockDataSource.getConnection()).thenReturn(dsConnection);
    when(dsConnection.unwrap(CalciteConnection.class))
        .thenThrow(new SQLException("not a CalciteConnection"));

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "MYCATALOG", "MYSCHEMA", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    assertFalse(schema.isLogical());
  }

  // Connection acquisition failure must be swallowed — detection is best-effort and must not break planning.
  @Test
  void testIsLogicalReturnsFalseWhenDataSourceConnectionFails() throws Exception {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("boom"));

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", null, "LOGICAL", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    assertFalse(schema.isLogical());
  }

  // Repeated calls must reuse the cached result rather than reopening the downstream connection each time.
  @Test
  void testIsLogicalIsMemoizedAcrossCalls() throws Exception {
    SchemaPlus downstreamRoot = Frameworks.createRootSchema(true);
    downstreamRoot.add("LOGICAL", new MarkerSchema());

    HoptimatorJdbcSchema schema = schemaWithDownstream(null, "LOGICAL", downstreamRoot);

    assertTrue(schema.isLogical());
    assertTrue(schema.isLogical());
    assertTrue(schema.isLogical());

    verify(mockDataSource, times(1)).getConnection();
  }

  @Test
  void testTablesGetReturnsNullForMissingTableAfterLoad() throws Exception {
    Connection dsConnection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(dsConnection);
    when(dsConnection.getMetaData()).thenReturn(metaData);
    when(metaData.getTables(anyString(), anyString(), anyString(), isNull())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(1)).thenReturn("myCatalog");
    when(resultSet.getString(2)).thenReturn("mySchema");
    when(resultSet.getString(3)).thenReturn("TABLE_A");
    when(resultSet.getString(4)).thenReturn("TABLE");

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);
    HoptimatorJdbcSchema schema = new HoptimatorJdbcSchema(
        "myDb", "myCatalog", "mySchema", mockDataSource,
        AnsiSqlDialect.DEFAULT, convention, Collections.emptyList());

    // Trigger loadAllTables first by asking for names
    schema.tables().getNames(LikePattern.any());
    // Now ask for a specific table that does not exist → exercises the null return path
    Table missing = schema.tables().get("NONEXISTENT_TABLE");

    assertNull(missing);
  }
}

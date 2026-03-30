package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcCatalogSchema;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class K8sDatabaseTableTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private K8sContext mockContext;

  @Mock
  private K8sEngineTable mockEngineTable;

  @Mock
  private MockedStatic<HoptimatorJdbcCatalogSchema> mockedCatalogSchema;

  @Mock
  private MockedStatic<HoptimatorJdbcSchema> mockedJdbcSchema;

  @Test
  void toRowMapsAllFields() {
    V1alpha1Database db = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("my-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:test://host")
            .catalog("cat")
            .schema("sch")
            .dialect(V1alpha1DatabaseSpec.DialectEnum.MYSQL)
            .driver("com.mysql.Driver"));

    K8sDatabaseTable table = new K8sDatabaseTable(null, null);
    K8sDatabaseTable.Row row = table.toRow(db);

    assertEquals("my-db", row.NAME);
    assertEquals("jdbc:test://host", row.URL);
    assertEquals("cat", row.CATALOG);
    assertEquals("sch", row.SCHEMA);
    assertEquals("MySQL", row.DIALECT);
    assertEquals("com.mysql.Driver", row.DRIVER);
  }

  @Test
  void toRowWithNullDialect() {
    V1alpha1Database db = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("db"))
        .spec(new V1alpha1DatabaseSpec().url("jdbc:test://host").dialect(null));

    K8sDatabaseTable table = new K8sDatabaseTable(null, null);
    K8sDatabaseTable.Row row = table.toRow(db);

    assertNull(row.DIALECT);
  }

  @Test
  void fromRowSetsK8sFields() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("my-db", "jdbc:test://host", "cat", "sch", "ANSI", "com.test.Driver");

    K8sDatabaseTable table = new K8sDatabaseTable(null, null);
    V1alpha1Database db = table.fromRow(row);

    assertEquals("my-db", db.getMetadata().getName());
    assertEquals("jdbc:test://host", db.getSpec().getUrl());
    assertEquals("cat", db.getSpec().getCatalog());
    assertEquals("sch", db.getSpec().getSchema());
    assertEquals("com.test.Driver", db.getSpec().getDriver());
  }

  @Test
  void fromRowWithInvalidNameThrows() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("INVALID_NAME", "jdbc:test://host", null, null, null, null);

    K8sDatabaseTable table = new K8sDatabaseTable(null, null);

    assertThrows(IllegalArgumentException.class, () -> table.fromRow(row));
  }

  @Test
  void fromRowSetsKindAndApiVersion() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("my-db", "jdbc:test://host", null, null, "ANSI", null);

    K8sDatabaseTable table = new K8sDatabaseTable(null, null);
    V1alpha1Database db = table.fromRow(row);

    assertEquals("Database", db.getKind());
    assertNotNull(db.getApiVersion());
  }

  @Test
  void toRowWithAnsiDialect() {
    V1alpha1Database db = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("db"))
        .spec(new V1alpha1DatabaseSpec().url("jdbc:test://host")
            .dialect(V1alpha1DatabaseSpec.DialectEnum.ANSI));

    K8sDatabaseTable table = new K8sDatabaseTable(null, null);
    K8sDatabaseTable.Row row = table.toRow(db);

    assertEquals("ANSI", row.DIALECT);
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sDatabaseTable table = new K8sDatabaseTable(null, null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }

  @Test
  void schemaNameReturnsSchemaWhenPresent() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "jdbc:test://host", null, "my-schema", null, null);
    assertEquals("my-schema", K8sDatabaseTable.schemaName(row));
  }

  @Test
  void schemaNameReturnsUppercaseNameWhenSchemaNull() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("my-db", "jdbc:test://host", null, null, null, null);
    assertEquals("MY-DB", K8sDatabaseTable.schemaName(row));
  }

  @Test
  void schemaNameReturnsUppercaseNameWhenSchemaEmpty() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("my-db", "jdbc:test://host", null, "", null, null);
    assertEquals("MY-DB", K8sDatabaseTable.schemaName(row));
  }

  @Test
  void dialectReturnsNullForNullDialect() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "url", null, null, null, null);
    assertNull(K8sDatabaseTable.dialect(row));
  }

  @Test
  void dialectReturnsAnsiForAnsi() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "url", null, null, "ANSI", null);
    SqlDialect result = K8sDatabaseTable.dialect(row);
    assertEquals(AnsiSqlDialect.DEFAULT, result);
  }

  @Test
  void dialectReturnsMysqlForMySQL() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "url", null, null, "MySQL", null);
    SqlDialect result = K8sDatabaseTable.dialect(row);
    assertEquals(MysqlSqlDialect.DEFAULT, result);
  }

  @Test
  void dialectReturnsCalciteForOther() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "url", null, null, "OTHER", null);
    SqlDialect result = K8sDatabaseTable.dialect(row);
    assertEquals(CalciteSqlDialect.DEFAULT, result);
  }

  @Test
  void dataSourceWithUserAndPassword() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "jdbc:test://host", null, null, null, "com.Test");
    Properties props = new Properties();
    props.setProperty("user", "myuser");
    props.setProperty("password", "mypass");

    DataSource ds = K8sDatabaseTable.dataSource(row, props);

    assertNotNull(ds);
  }

  @Test
  void dataSourceWithExtraProperties() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "jdbc:test://host", null, null, null, null);
    Properties props = new Properties();
    props.setProperty("extra.prop", "value");

    DataSource ds = K8sDatabaseTable.dataSource(row, props);

    assertNotNull(ds);
  }

  @Test
  void dataSourceWithUrlEndingInDoubleSlash() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "jdbc:test://", null, null, null, null);
    Properties props = new Properties();
    props.setProperty("key", "val");

    DataSource ds = K8sDatabaseTable.dataSource(row, props);

    assertNotNull(ds);
  }

  @Test
  void dataSourceWithNoProperties() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("db", "jdbc:test://host", null, null, null, null);
    Properties props = new Properties();

    DataSource ds = K8sDatabaseTable.dataSource(row, props);

    assertNotNull(ds);
  }

  @Test
  void rowConstructorSetsAllFields() {
    K8sDatabaseTable.Row row = new K8sDatabaseTable.Row("name", "url", "cat", "sch", "ANSI", "driver");
    assertEquals("name", row.NAME);
    assertEquals("url", row.URL);
    assertEquals("cat", row.CATALOG);
    assertEquals("sch", row.SCHEMA);
    assertEquals("ANSI", row.DIALECT);
    assertEquals("driver", row.DRIVER);
  }

  @Test
  void addDatabasesWithNoCatalog() throws Exception {
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("test-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:hoptimator://")
            .schema("TESTSCH")
            .dialect(V1alpha1DatabaseSpec.DialectEnum.ANSI)));

    lenient().when(mockEngineTable.forDatabase(anyString())).thenReturn(Collections.emptyList());
    Properties connProps = new Properties();
    doReturn(connProps).when(connection).connectionProperties();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    K8sDatabaseTable table = spy(new K8sDatabaseTable(mockContext, mockEngineTable));
    doReturn(databases.stream().map(table::toRow).collect(Collectors.toList())).when(table).rows();

    HoptimatorJdbcSchema mockSchema = mock(HoptimatorJdbcSchema.class);
    mockedJdbcSchema.when(() -> HoptimatorJdbcSchema.create(
        anyString(), any(), anyString(), any(DataSource.class), any(SchemaPlus.class),
        any(), anyList(), any())).thenReturn(mockSchema);

    table.addDatabases(root, connection);

    assertNotNull(root.subSchemas().get("TESTSCH"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void addDatabasesWithCatalog() throws Exception {
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("test-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:hoptimator://")
            .catalog("MYCAT")
            .schema("MYSCH")
            .dialect(V1alpha1DatabaseSpec.DialectEnum.ANSI)));

    lenient().when(mockEngineTable.forDatabase(anyString())).thenReturn(Collections.emptyList());
    Properties connProps = new Properties();
    doReturn(connProps).when(connection).connectionProperties();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    K8sDatabaseTable table = spy(new K8sDatabaseTable(mockContext, mockEngineTable));
    doReturn(databases.stream().map(table::toRow).collect(Collectors.toList())).when(table).rows();

    // Mock the catalog schema to return sub-schemas without hitting JDBC
    HoptimatorJdbcCatalogSchema mockCatalogSchema = mock(HoptimatorJdbcCatalogSchema.class);
    Lookup<Schema> mockSubSchemas = mock(Lookup.class);
    doReturn(mockSubSchemas).when(mockCatalogSchema).subSchemas();
    doReturn(Collections.emptySet()).when(mockSubSchemas).getNames(any(LikePattern.class));

    mockedCatalogSchema.when(() -> HoptimatorJdbcCatalogSchema.create(
        anyString(), anyString(), anyString(), any(DataSource.class), any(SchemaPlus.class),
        any(), anyList(), any())).thenReturn(mockCatalogSchema);

    table.addDatabases(root, connection);

    assertNotNull(root.subSchemas().get("MYCAT"));
  }

  @Test
  void addDatabasesWithNullSchemaUsesName() throws Exception {
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("test-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:hoptimator://")
            .dialect(V1alpha1DatabaseSpec.DialectEnum.MYSQL)));

    lenient().when(mockEngineTable.forDatabase(anyString())).thenReturn(Collections.emptyList());
    Properties connProps = new Properties();
    doReturn(connProps).when(connection).connectionProperties();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    K8sDatabaseTable table = spy(new K8sDatabaseTable(mockContext, mockEngineTable));
    doReturn(databases.stream().map(table::toRow).collect(Collectors.toList())).when(table).rows();

    HoptimatorJdbcSchema mockSchema = mock(HoptimatorJdbcSchema.class);
    mockedJdbcSchema.when(() -> HoptimatorJdbcSchema.create(
        anyString(), any(), any(), any(DataSource.class), any(SchemaPlus.class),
        any(), anyList(), any())).thenReturn(mockSchema);

    table.addDatabases(root, connection);

    // When schema is null, uses NAME.toUpperCase() = "TEST-DB"
    assertNotNull(root.subSchemas().get("TEST-DB"));
  }
}

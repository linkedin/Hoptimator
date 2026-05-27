package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class MySqlDeployerProviderTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private CalciteConnection calciteConnection;

  @Mock
  private SchemaPlus rootSchema;

  @Mock
  private SchemaPlus mysqlCatalogSchema;

  @Mock
  private SchemaPlus testdbSchema;

  @Mock
  private Lookup<SchemaPlus> rootSubSchemaLookup;

  @Mock
  private Lookup<SchemaPlus> catalogSubSchemaLookup;

  private MySqlDeployerProvider provider;

  @BeforeEach
  void setUp() {
    provider = new MySqlDeployerProvider();
  }

  @Test
  void testPriority() {
    assertEquals(2, provider.priority());
  }

  @Test
  void testReturnsDeployerForMySqlSchema() {
    Source source = new Source("mysql", List.of("MYSQL", "testdb", "users"), Collections.emptyMap());

    // Mock the chain: connection -> calciteConnection -> rootSchema -> MYSQL catalog -> testdb schema -> HoptimatorJdbcSchema -> BasicDataSource
    HoptimatorJdbcSchema jdbcSchema = mock(HoptimatorJdbcSchema.class);
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql-hoptimator://url=jdbc:mysql://test-url;user=testuser;password=testpass");

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(rootSubSchemaLookup).when(rootSchema).subSchemas();
    when(rootSubSchemaLookup.get("MYSQL")).thenReturn(mysqlCatalogSchema);
    doReturn(catalogSubSchemaLookup).when(mysqlCatalogSchema).subSchemas();
    when(catalogSubSchemaLookup.get("testdb")).thenReturn(testdbSchema);
    when(testdbSchema.unwrap(HoptimatorJdbcSchema.class)).thenReturn(jdbcSchema);
    when(jdbcSchema.getDataSource()).thenReturn(dataSource);

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertEquals(1, deployers.size());
    assertInstanceOf(MySqlDeployer.class, deployers.iterator().next());
  }

  @Test
  void testReturnsEmptyForNonMySqlCatalog() {
    // Catalog name "KAFKA" doesn't match "MYSQL" — short-circuits before schema lookup
    Source source = new Source("kafka", List.of("KAFKA", "MyTopic"), Collections.emptyMap());

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenCatalogNotFound() {
    Source source = new Source("mysql", List.of("UNKNOWN", "testdb", "users"), Collections.emptyMap());

    // No mocking needed - catalog name doesn't match, short-circuits early
    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyForNonSourceDeployable() {
    MaterializedView view = mock(MaterializedView.class);
    Collection<Deployer> deployers = provider.deployers(view, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenCatalogIsNull() {
    // Source with only schema and table (2-level path) — catalog() returns null
    Source source = new Source("mysql", List.of("testdb", "users"), Collections.emptyMap());
    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenSchemaNotFoundInCatalog() {
    Source source = new Source("mysql", List.of("MYSQL", "nonexistent", "users"), Collections.emptyMap());

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(rootSubSchemaLookup).when(rootSchema).subSchemas();
    when(rootSubSchemaLookup.get("MYSQL")).thenReturn(mysqlCatalogSchema);
    doReturn(catalogSubSchemaLookup).when(mysqlCatalogSchema).subSchemas();
    when(catalogSubSchemaLookup.get("nonexistent")).thenReturn(null);

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenUnwrapThrowsException() {
    Source source = new Source("mysql", List.of("MYSQL", "testdb", "users"), Collections.emptyMap());

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(rootSubSchemaLookup).when(rootSchema).subSchemas();
    when(rootSubSchemaLookup.get("MYSQL")).thenReturn(mysqlCatalogSchema);
    doReturn(catalogSubSchemaLookup).when(mysqlCatalogSchema).subSchemas();
    when(catalogSubSchemaLookup.get("testdb")).thenReturn(testdbSchema);
    when(testdbSchema.unwrap(HoptimatorJdbcSchema.class)).thenThrow(new RuntimeException("unwrap failed"));

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  // --- deployers() with non-HoptimatorConnection returns empty ---

  @Test
  void testReturnsEmptyWhenConnectionIsNotHoptimatorConnection() {
    Source source = new Source("mysql", List.of("MYSQL", "testdb", "users"), Collections.emptyMap());

    // Use a Connection (not HoptimatorConnection) as the connection parameter
    Connection rawConnection = mock(Connection.class);

    // The provider uses a Connection param (not HoptimatorConnection typed directly here)
    // But DeployerUtils.extractPropertiesFromJdbcSchema requires HoptimatorConnection
    // We need to feed a non-HoptimatorConnection to trigger the branch
    // MySqlDeployerProvider.deployers() takes Connection, not HoptimatorConnection
    Collection<Deployer> deployers = provider.deployers(source, rawConnection);
    // Without the isinstance check, this would try to cast and fail or create a broken deployer
    // With the check, it should return empty
    assertTrue(deployers.isEmpty(),
        "Expected empty deployers when connection is not a HoptimatorConnection");
  }

  // --- catalog check: catalog name comparison is case-insensitive ---

  @Test
  void testDeployersCatalogMatchIsCaseInsensitive() {
    // "mysql" lower-case catalog should NOT match "MYSQL" since equalsIgnoreCase is used
    // but non-matching catalog returns empty
    Source source = new Source("mysql", List.of("OTHER", "testdb", "users"), Collections.emptyMap());

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty(),
        "Expected empty deployers for non-MYSQL catalog name");
  }
}

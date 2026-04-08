package com.linkedin.hoptimator.venice;

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
class VeniceDeployerProviderTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private CalciteConnection calciteConnection;

  @Mock
  private SchemaPlus rootSchema;

  @Mock
  private SchemaPlus veniceSubSchema;

  @Mock
  private Lookup<SchemaPlus> subSchemaLookup;

  private VeniceDeployerProvider provider;

  @BeforeEach
  void setUp() {
    provider = new VeniceDeployerProvider();
  }

  @Test
  void testPriority() {
    assertEquals(2, provider.priority());
  }

  @Test
  void testReturnsDeployerForVeniceSchema() {
    Source source = new Source("venice", List.of("VENICE", "MyStore"), Collections.emptyMap());

    // Mock the chain: connection -> calciteConnection -> rootSchema -> subSchema -> HoptimatorJdbcSchema -> BasicDataSource
    HoptimatorJdbcSchema jdbcSchema = mock(HoptimatorJdbcSchema.class);
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:venice://clusters=test-cluster;router.url=test-url");

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(subSchemaLookup).when(rootSchema).subSchemas();
    when(subSchemaLookup.get("VENICE")).thenReturn(veniceSubSchema);
    when(veniceSubSchema.unwrap(HoptimatorJdbcSchema.class)).thenReturn(jdbcSchema);
    when(jdbcSchema.getDataSource()).thenReturn(dataSource);

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertEquals(1, deployers.size());
    assertInstanceOf(VeniceDeployer.class, deployers.iterator().next());
  }

  @Test
  void testReturnsEmptyForNonVeniceDatabase() {
    // Database name "test" doesn't match "venice" — short-circuits before schema lookup
    Source source = new Source("test", List.of("TEST", "MyStore"), Collections.emptyMap());

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenSchemaNotFound() {
    Source source = new Source("venice", List.of("UNKNOWN", "MyStore"), Collections.emptyMap());

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(subSchemaLookup).when(rootSchema).subSchemas();
    when(subSchemaLookup.get("UNKNOWN")).thenReturn(null);

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
  void testReturnsEmptyWhenSchemaNameIsNull() {
    // Source with only a table name (single-element path) — schema() returns null
    Source source = new Source("venice", List.of("MyStore"), Collections.emptyMap());
    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenDatabaseIsNull() {
    Source source = new Source(null, List.of("VENICE", "MyStore"), Collections.emptyMap());
    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenConnectionIsNotHoptimatorConnection() {
    // Use a plain Connection mock (not HoptimatorConnection) → should return empty
    Source source = new Source("venice", List.of("VENICE", "MyStore"), Collections.emptyMap());
    Connection plainConnection = mock(Connection.class);
    Collection<Deployer> deployers = provider.deployers(source, plainConnection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsDeployerForCaseInsensitiveVeniceDatabase() {
    // equalsIgnoreCase — database name "venice" (lowercase) should still match CATALOG_NAME "VENICE"
    Source source = new Source("venice", List.of("VENICE", "MyStore"), Collections.emptyMap());

    HoptimatorJdbcSchema jdbcSchema = mock(HoptimatorJdbcSchema.class);
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:venice://clusters=test-cluster;router.url=test-url");

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(subSchemaLookup).when(rootSchema).subSchemas();
    when(subSchemaLookup.get("VENICE")).thenReturn(veniceSubSchema);
    when(veniceSubSchema.unwrap(HoptimatorJdbcSchema.class)).thenReturn(jdbcSchema);
    when(jdbcSchema.getDataSource()).thenReturn(dataSource);

    Collection<Deployer> deployers = provider.deployers(source, connection);
    // Source database is "venice" (lowercase) which equalsIgnoreCase "VENICE"
    assertEquals(1, deployers.size());
    assertInstanceOf(VeniceDeployer.class, deployers.iterator().next());
  }

  @Test
  void testReturnsEmptyWhenUnwrapThrowsException() {
    Source source = new Source("venice", List.of("VENICE", "MyStore"), Collections.emptyMap());

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    doReturn(subSchemaLookup).when(rootSchema).subSchemas();
    when(subSchemaLookup.get("VENICE")).thenReturn(veniceSubSchema);
    when(veniceSubSchema.unwrap(HoptimatorJdbcSchema.class)).thenThrow(new RuntimeException("unwrap failed"));

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }
}

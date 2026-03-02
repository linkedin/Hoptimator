package com.linkedin.hoptimator.kafka;

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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class KafkaDeployerProviderTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private CalciteConnection calciteConnection;

  @Mock
  private SchemaPlus rootSchema;

  @Mock
  private SchemaPlus topicSubSchema;

  @Mock
  @SuppressWarnings("rawtypes")
  private Lookup subSchemaLookup;

  private KafkaDeployerProvider provider;

  @BeforeEach
  void setUp() {
    provider = new KafkaDeployerProvider();
  }

  @Test
  void testPriority() {
    assertEquals(2, provider.priority());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testReturnsDeployerForKafkaSchema() {
    Source source = new Source("kafka-database", List.of("KAFKA", "MyTopic"), Collections.emptyMap());

    // Mock the chain: connection -> calciteConnection -> rootSchema -> subSchema -> HoptimatorJdbcSchema -> BasicDataSource
    HoptimatorJdbcSchema jdbcSchema = mock(HoptimatorJdbcSchema.class);
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:kafka://bootstrap.servers=test-url");

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.subSchemas()).thenReturn(subSchemaLookup);
    when(subSchemaLookup.get("KAFKA")).thenReturn(topicSubSchema);
    when(topicSubSchema.unwrap(HoptimatorJdbcSchema.class)).thenReturn(jdbcSchema);
    when(jdbcSchema.getDataSource()).thenReturn(dataSource);

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof KafkaDeployer);
  }

  @Test
  void testReturnsEmptyForNonKafkaDatabase() {
    // Database name "test" doesn't start with "kafka" — short-circuits before schema lookup
    Source source = new Source("test", List.of("TEST", "MyStore"), Collections.emptyMap());

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testReturnsEmptyWhenSchemaNotFound() {
    Source source = new Source("kafka-unknown", List.of("UNKNOWN", "MyTable"), Collections.emptyMap());

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.subSchemas()).thenReturn(subSchemaLookup);
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
    Source source = new Source("kafka-database", List.of("MyTopic"), Collections.emptyMap());
    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @Test
  void testReturnsEmptyWhenDatabaseIsNull() {
    Source source = new Source(null, List.of("KAFKA", "MyTopic"), Collections.emptyMap());
    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testReturnsEmptyWhenUnwrapThrowsException() {
    Source source = new Source("kafka-database", List.of("KAFKA", "MyTopic"), Collections.emptyMap());

    when(connection.calciteConnection()).thenReturn(calciteConnection);
    when(calciteConnection.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.subSchemas()).thenReturn(subSchemaLookup);
    when(subSchemaLookup.get("KAFKA")).thenReturn(topicSubSchema);
    when(topicSubSchema.unwrap(HoptimatorJdbcSchema.class)).thenThrow(new RuntimeException("unwrap failed"));

    Collection<Deployer> deployers = provider.deployers(source, connection);
    assertTrue(deployers.isEmpty());
  }
}

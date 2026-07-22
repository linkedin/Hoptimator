package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.util.DeploymentService;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the SQL-free direct {@link TableService} API.
 */
@ExtendWith(MockitoExtension.class)
class TableServiceTest {

  private static final String RECORD_SCHEMA = "{"
      + "\"type\":\"record\",\"name\":\"MyTable\",\"namespace\":\"com.linkedin.test\","
      + "\"fields\":["
      + "{\"name\":\"id\",\"type\":\"long\"},"
      + "{\"name\":\"name\",\"type\":\"string\"}"
      + "]}";

  private final List<String> path = List.of("UTIL", "myTable");

  @Mock
  private MockedStatic<DatabaseConfigResolvers> resolvers;

  @Mock
  private MockedStatic<DeploymentService> deployment;

  @Test
  void createDryRunDerivesRowTypeFromAvroSchema() throws SQLException {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    HoptimatorDdlUtils.SpecifyResult result =
        TableService.create(new Properties(), Collections.emptyList(), path, recordSchema(),
            Collections.emptyMap(), false, true);

    assertThat(result).isNotNull();
    assertThat(result.viewPath).endsWith("myTable");

    RelDataType rowType = result.sinkRowType;
    assertThat(rowType.isStruct()).isTrue();
    assertThat(rowType.getFieldNames()).containsExactly("id", "name");
    assertThat(rowType.getField("id", false, false).getType().getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
    assertThat(rowType.getField("name", false, false).getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  void createRejectsPathWithoutDatabaseAndTable() {
    assertThatThrownBy(() ->
        TableService.create(new Properties(), Collections.emptyList(), List.of("onlyOne"), recordSchema(),
            Collections.emptyMap(), false, true))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("database and a table name");
  }

  @Test
  void createRejectsNullSchema() {
    assertThatThrownBy(() ->
        TableService.create(new Properties(), Collections.emptyList(), path, null,
            Collections.emptyMap(), false, true))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("Avro schema is required");
  }

  @Test
  void rejectsNonRecordSchema() {
    Schema primitive = Schema.create(Schema.Type.STRING);
    assertThatThrownBy(() ->
        TableService.create(new Properties(), Collections.emptyList(), path, primitive,
            Collections.emptyMap(), false, true))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("must be a record");
  }

  @Test
  void deleteRejectsPathWithoutDatabaseAndTable() {
    assertThatThrownBy(() -> TableService.delete(new Properties(), List.of("onlyOne")))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("database and a table name");
  }

  @Test
  void deleteRunsValidationAndDeployerTeardown() throws SQLException {
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = Collections.singletonList(deployer);
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(deployers);

    TableService.delete(new Properties(), path);

    deployment.verify(() -> DeploymentService.delete(deployers), times(1));
    deployment.verify(() -> DeploymentService.restore(any()), never());
  }

  @Test
  void deleteRestoresAndRethrowsWhenTeardownFails() {
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = Collections.singletonList(deployer);
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(deployers);
    deployment.when(() -> DeploymentService.delete(deployers))
        .thenThrow(new SQLException("teardown boom"));

    assertThatThrownBy(() -> TableService.delete(new Properties(), path))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("teardown boom");

    deployment.verify(() -> DeploymentService.restore(deployers), times(1));
  }

  private static Schema recordSchema() {
    return new Schema.Parser().parse(RECORD_SCHEMA);
  }

  private DatabaseConfigResolver stubResolver() {
    DatabaseConfigResolver resolver = mock(DatabaseConfigResolver.class);
    try {
      when(resolver.databaseName(path)).thenReturn("test-database");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return resolver;
  }
}

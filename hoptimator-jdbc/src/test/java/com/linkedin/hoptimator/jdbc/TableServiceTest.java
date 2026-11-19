package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.DeployerScope;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;


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

  @BeforeEach
  void setUp() throws SQLException {
    deployment.when(() -> DeploymentService.deployers(any(), any()))
        .thenAnswer(invocation -> new DeployerScope(List.of()));
  }

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
        .thenReturn(new DeployerScope(deployers));

    TableService.delete(new Properties(), path);

    deployment.verify(() -> DeploymentService.delete(deployers), times(1));
    deployment.verify(() -> DeploymentService.restore(any()), never());
    verify(deployer).close();
  }

  @Test
  void deleteMergesConnectionHintsIntoSourceOptions() throws SQLException {
    // Connection hints must reach deployers via source.options() on delete just as they do on create;
    // an empty options map would strip hints a deployer's teardown may depend on.
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = Collections.singletonList(deployer);
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    deployment.when(() -> DeploymentService.parseHints(any()))
        .thenReturn(Map.of("hintKey", "hintValue"));
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(deployers));

    TableService.delete(new Properties(), path);

    ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
    deployment.verify(() ->
        DeploymentService.deployers(sourceCaptor.capture(), any(DeploymentContext.class)));
    assertThat(sourceCaptor.getValue().options()).containsEntry("hintKey", "hintValue");
  }

  @Test
  void deleteRestoresAndRethrowsWhenTeardownFails() {
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = Collections.singletonList(deployer);
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(deployers));
    deployment.when(() -> DeploymentService.delete(deployers))
        .thenThrow(new SQLException("teardown boom"));
    deployment.when(() -> DeploymentService.restore(deployers)).thenAnswer(invocation -> {
      verify(deployer, never()).close();
      return null;
    });

    assertThatThrownBy(() -> TableService.delete(new Properties(), path))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("teardown boom");

    deployment.verify(() -> DeploymentService.restore(deployers), times(1));
    verify(deployer).close();
  }

  @Test
  void deletePreservesTeardownFailureWhenCloseAlsoFails() {
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = List.of(deployer);
    SQLException operationFailure = new SQLException("teardown failed");
    RuntimeException closeFailure = new IllegalStateException("close failed");
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(deployers));
    deployment.when(() -> DeploymentService.delete(deployers)).thenThrow(operationFailure);
    deployment.when(() -> DeploymentService.restore(deployers)).thenAnswer(invocation -> {
      verify(deployer, never()).close();
      return null;
    });
    doThrow(closeFailure).when(deployer).close();

    assertThatThrownBy(() -> TableService.delete(new Properties(), path))
        .isSameAs(operationFailure)
        .satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(closeFailure));

    deployment.verify(() -> DeploymentService.restore(deployers));
    verify(deployer).close();
  }

  @Test
  void createDryRunRendersWithoutCreatingUpdatingOrDeleting() throws SQLException {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = List.of(deployer);
    deployment.when(() -> DeploymentService.deployers(any(), any())).thenReturn(new DeployerScope(deployers));
    when(deployer.specify()).thenReturn(List.of("table spec"));

    HoptimatorDdlUtils.SpecifyResult result = TableService.create(new Properties(), List.of(), path,
        recordSchema(), Map.of(), true, true);

    assertThat(result.specs).containsExactly("table spec");
    deployment.verify(() -> DeploymentService.create(any()), never());
    deployment.verify(() -> DeploymentService.update(any()), never());
    deployment.verify(() -> DeploymentService.delete(any()), never());
    deployment.verify(() -> DeploymentService.restore(deployers));
    verify(deployer, never()).create();
    verify(deployer, never()).update();
    verify(deployer, never()).delete();
    verify(deployer).close();
  }

  @Test
  void createPreservesDeploymentFailureWhenCloseAlsoFails() {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = List.of(deployer);
    SQLException operationFailure = new SQLException("create failed");
    RuntimeException closeFailure = new IllegalStateException("close failed");
    deployment.when(() -> DeploymentService.deployers(any(), any())).thenReturn(new DeployerScope(deployers));
    deployment.when(() -> DeploymentService.create(deployers)).thenThrow(operationFailure);
    deployment.when(() -> DeploymentService.restore(deployers)).thenAnswer(invocation -> {
      verify(deployer, never()).close();
      return null;
    });
    doThrow(closeFailure).when(deployer).close();

    assertThatThrownBy(() -> TableService.create(new Properties(), List.of(), path,
        recordSchema(), Map.of(), false, false))
        .isSameAs(operationFailure)
        .satisfies(failure -> assertThat(failure.getSuppressed()).containsExactly(closeFailure));

    deployment.verify(() -> DeploymentService.restore(deployers));
    verify(deployer).close();
  }

  @ParameterizedTest
  @EnumSource(HoptimatorDdlUtils.DdlMode.class)
  void createModesReleaseDeployersAfterPollingOrDryRun(HoptimatorDdlUtils.DdlMode mode) throws SQLException {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = List.of(deployer);
    deployment.when(() -> DeploymentService.deployers(any(), any())).thenReturn(new DeployerScope(deployers));
    if (mode.dryRun()) {
      when(deployer.specify()).thenAnswer(invocation -> {
        verify(deployer, never()).close();
        return List.of("table spec");
      });
      deployment.when(() -> DeploymentService.restore(deployers)).thenAnswer(invocation -> {
        verify(deployer, never()).close();
        return null;
      });
    } else if (mode == HoptimatorDdlUtils.DdlMode.CREATE) {
      deployment.when(() -> DeploymentService.create(deployers)).thenAnswer(invocation -> {
        verify(deployer, never()).close();
        return null;
      });
    } else {
      deployment.when(() -> DeploymentService.update(deployers)).thenAnswer(invocation -> {
        verify(deployer, never()).close();
        return null;
      });
    }

    HoptimatorDdlUtils.SpecifyResult result = TableService.create(new Properties(), List.of(), path,
        recordSchema(), Map.of(), mode == HoptimatorDdlUtils.DdlMode.UPDATE, mode.dryRun());

    assertThat(result.specs).isEqualTo(mode.dryRun() ? List.of("table spec") : List.of());
    verify(deployer).close();
  }

  @Test
  void cleanupFailureDoesNotRestoreSuccessfulDirectCreate() throws SQLException {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    RuntimeException expected = new IllegalStateException("close failed");
    doThrow(expected).when(deployer).close();
    deployment.when(() -> DeploymentService.deployers(any(), any()))
        .thenReturn(new DeployerScope(List.of(deployer)));

    assertThatThrownBy(() -> TableService.create(new Properties(), List.of(), path,
        recordSchema(), Map.of(), false, false)).isSameAs(expected);

    deployment.verify(() -> DeploymentService.create(List.of(deployer)));
    deployment.verify(() -> DeploymentService.restore(any()), never());
  }

  @Test
  void createFailsWhenTableAlreadyExists() throws SQLException {
    // Direct-path CREATE (updateIfExists=false) against an existing table must fail, mirroring the
    // SQL path's "already exists, use OR REPLACE" instead of silently skipping per deployer.
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    when(deployer.exists()).thenReturn(true);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(Collections.singletonList(deployer)));

    assertThatThrownBy(() -> TableService.create(new Properties(), Collections.emptyList(), path,
        recordSchema(), Collections.emptyMap(), false, false))
        .isInstanceOf(SQLNonTransientException.class)
        .hasMessageContaining("already exists");

    deployment.verify(() -> DeploymentService.create(any()), never());
  }

  @Test
  void createProceedsWhenTableDoesNotExist() throws SQLException {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    when(deployer.exists()).thenReturn(false);
    List<Deployer> deployers = Collections.singletonList(deployer);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(deployers));

    TableService.create(new Properties(), Collections.emptyList(), path, recordSchema(),
        Collections.emptyMap(), false, false);

    deployment.verify(() -> DeploymentService.create(deployers), times(1));
  }

  @Test
  void updateIfExistsBypassesTheGuardAndDoesNotConsultExists() throws SQLException {
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    Deployer deployer = mock(Deployer.class);
    List<Deployer> deployers = Collections.singletonList(deployer);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(deployers));

    TableService.create(new Properties(), Collections.emptyList(), path, recordSchema(),
        Collections.emptyMap(), true, false);

    deployment.verify(() -> DeploymentService.update(deployers), times(1));
    verify(deployer, never()).exists();
  }

  @Test
  void createMergesConnectionHintsIntoTableOptionsWithHintsWinning() throws SQLException {
    // Connection hints must be merged into the table options handed to the deployers, and a hint
    // must override a caller-supplied option of the same key so callers can't override
    // connection-level properties (impersonation guard).
    DatabaseConfigResolver resolver = stubResolver();
    resolvers.when(() -> DatabaseConfigResolvers.forProperties(any())).thenReturn(resolver);
    deployment.when(() -> DeploymentService.parseHints(any()))
        .thenReturn(Map.of("owner", "connection-user", "hintOnly", "hintValue"));
    Deployer deployer = mock(Deployer.class);
    when(deployer.exists()).thenReturn(false);
    List<Deployer> deployers = Collections.singletonList(deployer);
    deployment.when(() -> DeploymentService.deployers(any(Source.class), any(DeploymentContext.class)))
        .thenReturn(new DeployerScope(deployers));

    Map<String, String> callerOptions = Map.of("owner", "caller-attempt", "callerOnly", "callerValue");
    TableService.create(new Properties(), Collections.emptyList(), path, recordSchema(),
        callerOptions, false, false);

    ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
    deployment.verify(() ->
        DeploymentService.deployers(sourceCaptor.capture(), any(DeploymentContext.class)));
    Map<String, String> mergedOptions = sourceCaptor.getValue().options();
    assertThat(mergedOptions).containsEntry("callerOnly", "callerValue");
    assertThat(mergedOptions).containsEntry("hintOnly", "hintValue");
    assertThat(mergedOptions).containsEntry("owner", "connection-user");
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

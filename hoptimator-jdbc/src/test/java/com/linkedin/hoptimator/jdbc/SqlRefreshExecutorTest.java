package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.util.DeploymentService;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;


@ExtendWith(MockitoExtension.class)
class SqlRefreshExecutorTest {

  @Mock
  MockedStatic<DeploymentService> mockDeploymentService;

  @Mock
  MockedStatic<ValidationService> mockValidationService;

  @Mock
  MockedStatic<RefreshService> mockRefreshService;

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    connection = (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties());
    mockDeploymentService.when(() -> DeploymentService.deployers(any(), any()))
        .thenReturn(Collections.emptyList());
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  private void execute(String sql) {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, sql);
    executor.executeDdl(context, node);
  }

  private void stubResolve(RefreshTarget target) throws SQLException {
    mockRefreshService.when(() -> RefreshService.resolve(any(), any())).thenReturn(target);
  }

  @Test
  void firesEveryUpstreamTrigger() throws SQLException {
    stubResolve(new RefreshTarget(RefreshTarget.Kind.MATERIALIZED_VIEW, Arrays.asList("t1", "t2", "t3")));

    assertDoesNotThrow(() -> execute("REFRESH \"foo\""));

    // One deploy/update per upstream trigger.
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(3));
  }

  @Test
  void windowedRefreshFiresEachTrigger() throws SQLException {
    stubResolve(new RefreshTarget(RefreshTarget.Kind.TABLE, Arrays.asList("t1", "t2")));

    assertDoesNotThrow(() -> execute("REFRESH TABLE \"foo\" FROM '2026-05-01' TO '2026-05-08'"));

    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(2));
  }

  @Test
  void unknownObjectThrows() throws SQLException {
    stubResolve(null);

    HoptimatorDdlExecutor.DdlException ex = assertThrows(HoptimatorDdlExecutor.DdlException.class,
        () -> execute("REFRESH \"foo\""));
    assertTrue(ex.getMessage().contains("no such materialized view or logical table"));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(0));
  }

  @Test
  void noUpstreamTriggersThrows() throws SQLException {
    stubResolve(new RefreshTarget(RefreshTarget.Kind.TABLE, Collections.emptyList()));

    HoptimatorDdlExecutor.DdlException ex = assertThrows(HoptimatorDdlExecutor.DdlException.class,
        () -> execute("REFRESH \"foo\""));
    assertTrue(ex.getMessage().contains("no upstream triggers"));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(0));
  }

  @Test
  void kindMismatchTableAssertedOnMaterializedViewThrows() throws SQLException {
    stubResolve(new RefreshTarget(RefreshTarget.Kind.MATERIALIZED_VIEW, Collections.singletonList("t1")));

    HoptimatorDdlExecutor.DdlException ex = assertThrows(HoptimatorDdlExecutor.DdlException.class,
        () -> execute("REFRESH TABLE \"foo\""));
    assertTrue(ex.getMessage().contains("materialized view"));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(0));
  }

  @Test
  void kindMismatchMaterializedViewAssertedOnTableThrows() throws SQLException {
    stubResolve(new RefreshTarget(RefreshTarget.Kind.TABLE, Collections.singletonList("t1")));

    HoptimatorDdlExecutor.DdlException ex = assertThrows(HoptimatorDdlExecutor.DdlException.class,
        () -> execute("REFRESH MATERIALIZED VIEW \"foo\""));
    assertTrue(ex.getMessage().contains("a table"));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(0));
  }

  @Test
  void matchingKindAssertionSucceeds() throws SQLException {
    stubResolve(new RefreshTarget(RefreshTarget.Kind.TABLE, Collections.singletonList("t1")));

    assertDoesNotThrow(() -> execute("REFRESH TABLE \"foo\""));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(1));
  }
}

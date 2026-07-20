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

  @Test
  void firesEveryProducingTrigger() throws SQLException {
    mockRefreshService.when(() -> RefreshService.producingTriggers(any(), any()))
        .thenReturn(Arrays.asList("t1", "t2"));

    assertDoesNotThrow(() -> execute("REFRESH \"ADS\".\"MEMBERS\""));

    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(2));
  }

  @Test
  void windowedRefreshFiresProducingTrigger() throws SQLException {
    mockRefreshService.when(() -> RefreshService.producingTriggers(any(), any()))
        .thenReturn(Collections.singletonList("t1"));

    assertDoesNotThrow(() -> execute("REFRESH \"ADS\".\"MEMBERS\" FROM '2026-05-01' TO '2026-05-08'"));

    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(1));
  }

  @Test
  void noProducingTriggerThrows() throws SQLException {
    mockRefreshService.when(() -> RefreshService.producingTriggers(any(), any()))
        .thenReturn(Collections.emptyList());

    HoptimatorDdlExecutor.DdlException ex = assertThrows(HoptimatorDdlExecutor.DdlException.class,
        () -> execute("REFRESH \"ADS\".\"MEMBERS\""));
    assertTrue(ex.getMessage().contains("no trigger produces it"));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(0));
  }

  @Test
  void surfacesResolveError() throws SQLException {
    mockRefreshService.when(() -> RefreshService.producingTriggers(any(), any()))
        .thenThrow(new SQLException("FOO is a logical table; REFRESH a specific physical table (tier) instead."));

    HoptimatorDdlExecutor.DdlException ex = assertThrows(HoptimatorDdlExecutor.DdlException.class,
        () -> execute("REFRESH \"FOO\""));
    assertTrue(ex.getMessage().contains("logical table"));
    mockDeploymentService.verify(() -> DeploymentService.update(any()), times(0));
  }
}

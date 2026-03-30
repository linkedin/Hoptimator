package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTable;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTrigger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDdlExecutorValidationTest {

  @Mock
  MockedStatic<ValidationService> mockValidationService;

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    Properties props = new Properties();
    connection = (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", props);

    mockValidationService.when(() -> ValidationService.validateOrThrow(any()))
        .thenThrow(new SQLException("validation failed"));
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  void testCreateViewValidationFailure() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateView node = (SqlCreateView) HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"vf\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testCreateTriggerValidationFailure() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateTrigger node = (SqlCreateTrigger) HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"tf\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testCreateTableValidationFailure() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateTable node = (SqlCreateTable) HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"tf\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }
}

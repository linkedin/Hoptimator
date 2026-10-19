package com.linkedin.hoptimator.pinot;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class PinotDriverTest {

  @Mock
  private PinotSchema mockSchema;

  private PinotDriver driver;

  @BeforeEach
  void setUp() {
    driver = new PinotDriver() {
      @Override
      protected PinotSchema createPinotSchema(Properties properties) {
        return mockSchema;
      }
    };
  }

  @Test
  void getConnectStringPrefix() {
    assertThat(driver.getConnectStringPrefix()).isEqualTo("jdbc:pinot://");
  }

  @Test
  void createDriverVersion() {
    DriverVersion version = driver.createDriverVersion();
    assertThat(version).isNotNull();
    assertThat(version.productName).isEqualTo("pinot");
  }

  @Test
  void connectReturnsNullForNonMatchingUrl() throws Exception {
    assertThat(driver.connect("jdbc:other://host", new Properties())).isNull();
  }

  @Test
  void connectRegistersSchemaAndSetsCatalog() throws Exception {
    try (Connection connection = driver.connect("jdbc:pinot://controllerUrl=http://localhost:9000",
        new Properties())) {
      assertThat(connection).isNotNull();
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      assertThat(calciteConnection.getCatalog()).isEqualTo("PINOT");
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      assertThat(rootSchema.subSchemas().get("PINOT")).isNotNull();
      assertThat(connection.getAutoCommit()).isTrue();
    }
  }

  @Test
  void createPinotSchemaBuildsFromControllerUrl() {
    Properties props = new Properties();
    props.setProperty(PinotDriver.CONTROLLER_URL, "http://localhost:9000");
    assertThat(new PinotDriver().createPinotSchema(props)).isNotNull();
  }

  @Test
  void connectThrowsSqlExceptionOnFailure() {
    PinotDriver failingDriver = new PinotDriver() {
      @Override
      protected PinotSchema createPinotSchema(Properties properties) {
        throw new RuntimeException("infrastructure error");
      }
    };

    assertThatThrownBy(() -> failingDriver.connect("jdbc:pinot://controllerUrl=http://localhost:9000",
        new Properties()))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("jdbc:pinot://");
  }
}

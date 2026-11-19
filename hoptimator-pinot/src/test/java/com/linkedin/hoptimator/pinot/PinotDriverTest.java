package com.linkedin.hoptimator.pinot;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import com.linkedin.hoptimator.jdbc.HoptimatorCalciteSchema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
      verify(mockSchema, never()).close();
    }
    verify(mockSchema).close();
  }

  @Test
  void poolReturnKeepsClientUntilPhysicalDisposal() throws Exception {
    List<PinotSchema> created = new ArrayList<>();
    PinotDriver pooledDriver = new PinotDriver() {
      @Override protected PinotSchema createPinotSchema(Properties properties) {
        PinotSchema schema = mock(PinotSchema.class);
        created.add(schema);
        return schema;
      }
    };
    try (BasicDataSource pool = new BasicDataSource()) {
      pool.setDriver(pooledDriver);
      pool.setUrl("jdbc:pinot://");
      pool.setMaxTotal(1);
      Connection lease = pool.getConnection();
      PinotSchema owned = created.get(created.size() - 1);
      lease.close();
      verify(owned, never()).close();
      try (Connection reused = pool.getConnection()) {
        assertThat(reused.getCatalog()).isEqualTo("PINOT");
        verify(owned, never()).close();
      }
      pool.close();
      pool.close();
      verify(owned).close();
    }
    for (PinotSchema schema : created) {
      verify(schema).close();
    }
  }

  @Test
  void failedSchemaConstructionClosesPhysicalConnection() throws SQLException {
    AtomicReference<AvaticaConnection> created = new AtomicReference<>();
    RuntimeException expected = new IllegalArgumentException("schema failed");
    PinotDriver failing = new PinotDriver() {
      @Override protected void onConnectionInit(AvaticaConnection connection) throws SQLException {
        created.set(connection);
        super.onConnectionInit(connection);
      }

      @Override protected PinotSchema createPinotSchema(Properties properties) {
        throw expected;
      }
    };

    assertThatThrownBy(() -> failing.connect("jdbc:pinot://", new Properties()))
        .isInstanceOf(SQLException.class).hasCause(expected);
    assertThat(created.get().isClosed()).isTrue();
  }

  @Test
  void failedPublicationClosesRegisteredSchemaAndPreservesFailure() throws Exception {
    RuntimeException expected = new IllegalArgumentException("publication failed");
    PinotDriver failing = new PinotDriver() {
      @Override protected CalciteSchema createRootSchema(Properties properties) {
        return new HoptimatorCalciteSchema(null, new AbstractSchema(), "") {
          @Override public CalciteSchema add(String name, Schema schema) {
            if (CATALOG_NAME.equals(name)) {
              throw expected;
            }
            return super.add(name, schema);
          }
        };
      }

      @Override protected PinotSchema createPinotSchema(Properties properties) {
        return mockSchema;
      }
    };

    assertThatThrownBy(() -> failing.connect("jdbc:pinot://", new Properties()))
        .isInstanceOf(SQLException.class).hasCause(expected);
    verify(mockSchema).close();
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

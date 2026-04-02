package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.sql.Wrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class K8sCatalogTest {

  @Mock
  private MockedStatic<K8sContext> mockedK8sContext;

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private K8sContext mockContext;

  @Mock
  private K8sDatabaseTable mockDatabaseTable;

  @Mock
  private K8sViewTable mockViewTable;

  @Mock
  private Wrapper wrapper;

  @Test
  void nameReturnsK8s() {
    K8sCatalog catalog = new K8sCatalog();
    assertEquals("k8s", catalog.name());
  }

  @Test
  void descriptionReturnsExpected() {
    K8sCatalog catalog = new K8sCatalog();
    assertEquals("K8s catalog", catalog.description());
  }

  @Test
  void registerAddsMetadataAndDatabases() throws SQLException {
    SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true).plus();
    doReturn(schemaPlus).when(wrapper).unwrap(SchemaPlus.class);
    doReturn(connection).when(wrapper).unwrap(HoptimatorConnection.class);
    mockedK8sContext.when(() -> K8sContext.create(any())).thenReturn(mockContext);

    // Use a subclass that returns our test metadata with mocked tables
    K8sCatalog catalog = new K8sCatalog() {
      @Override
      K8sMetadata createMetadata(HoptimatorConnection conn, K8sContext ctx) {
        return new K8sMetadata(conn, ctx) {
          @Override
          public K8sDatabaseTable databaseTable() {
            return mockDatabaseTable;
          }

          @Override
          public K8sViewTable viewTable() {
            return mockViewTable;
          }
        };
      }
    };

    catalog.register(wrapper);

    assertNotNull(schemaPlus.subSchemas().get("k8s"));
  }

  @Test
  void registerAddsViewsToSchema() throws SQLException {
    // Tests register() VoidMethodCall: metadata.viewTable().addViews(schemaPlus) must be called
    SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true).plus();
    doReturn(schemaPlus).when(wrapper).unwrap(SchemaPlus.class);
    doReturn(connection).when(wrapper).unwrap(HoptimatorConnection.class);
    mockedK8sContext.when(() -> K8sContext.create(any())).thenReturn(mockContext);

    K8sCatalog catalog = new K8sCatalog() {
      @Override
      K8sMetadata createMetadata(HoptimatorConnection conn, K8sContext ctx) {
        return new K8sMetadata(conn, ctx) {
          @Override
          public K8sDatabaseTable databaseTable() {
            return mockDatabaseTable;
          }

          @Override
          public K8sViewTable viewTable() {
            return mockViewTable;
          }
        };
      }
    };

    catalog.register(wrapper);

    // After register(), addViews() should have been called on the view table
    assertNotNull(schemaPlus.subSchemas().get("k8s"), "k8s schema must be registered");
    verify(mockViewTable).addViews(any(SchemaPlus.class));
  }

  @Test
  void registerAddsDatabasesViaAddDatabases() throws SQLException {
    // Tests register() VoidMethodCall: metadata.databaseTable().addDatabases(schemaPlus, conn)
    SchemaPlus schemaPlus = CalciteSchema.createRootSchema(true).plus();
    doReturn(schemaPlus).when(wrapper).unwrap(SchemaPlus.class);
    doReturn(connection).when(wrapper).unwrap(HoptimatorConnection.class);
    mockedK8sContext.when(() -> K8sContext.create(any())).thenReturn(mockContext);

    K8sCatalog catalog = new K8sCatalog() {
      @Override
      K8sMetadata createMetadata(HoptimatorConnection conn, K8sContext ctx) {
        return new K8sMetadata(conn, ctx) {
          @Override
          public K8sDatabaseTable databaseTable() {
            return mockDatabaseTable;
          }

          @Override
          public K8sViewTable viewTable() {
            return mockViewTable;
          }
        };
      }
    };

    catalog.register(wrapper);

    // mockDatabaseTable is a Mockito mock, so verify() works directly
    verify(mockDatabaseTable).addDatabases(any(SchemaPlus.class), any());
  }

  @Test
  void registerThrowsWhenUnwrapFails() throws SQLException {
    doThrow(new SQLException("Cannot unwrap")).when(wrapper).unwrap(SchemaPlus.class);

    K8sCatalog catalog = new K8sCatalog();
    assertThrows(SQLException.class, () -> catalog.register(wrapper));
  }
}

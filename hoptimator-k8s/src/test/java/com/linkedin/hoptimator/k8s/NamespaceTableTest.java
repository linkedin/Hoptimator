package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1NamespaceStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class NamespaceTableTest {

  @Mock
  private K8sApi<V1Namespace, V1NamespaceList> mockApi;

  private NamespaceTable table;

  @BeforeEach
  void setUp() {
    table = new NamespaceTable(mockApi);
  }

  @Test
  void toRowMapsNameAndStatus() {
    V1Namespace ns = new V1Namespace()
        .metadata(new V1ObjectMeta().name("my-namespace"))
        .status(new V1NamespaceStatus().phase("Active"));

    NamespaceTable.Row row = table.toRow(ns);

    assertEquals("my-namespace", row.NAME);
    assertEquals("Active", row.STATUS);
  }

  @Test
  void getElementTypeReturnsRowClass() {
    assertEquals(NamespaceTable.Row.class, table.getElementType());
  }

  @Test
  void getRowTypeReturnsNonNull() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
  }

  @Test
  void rowsReturnsCollectionFromApi() throws SQLException {
    V1Namespace ns1 = new V1Namespace()
        .metadata(new V1ObjectMeta().name("ns1"))
        .status(new V1NamespaceStatus().phase("Active"));
    V1Namespace ns2 = new V1Namespace()
        .metadata(new V1ObjectMeta().name("ns2"))
        .status(new V1NamespaceStatus().phase("Terminating"));

    when(mockApi.list()).thenReturn(Arrays.asList(ns1, ns2));

    Collection<NamespaceTable.Row> rows = table.rows();

    assertEquals(2, rows.size());
  }

  @Test
  void apiReturnsProvidedApi() {
    assertSame(mockApi, table.api());
  }

  @Test
  void fromRowThrowsUnsupportedOperation() {
    NamespaceTable.Row row = new NamespaceTable.Row("ns", "Active");
    assertThrows(UnsupportedOperationException.class, () -> table.fromRow(row));
  }

  @Test
  void getModifiableCollectionReturnsSameAsRows() {
    assertSame(table.rows(), table.getModifiableCollection());
  }

  @Test
  void contextConstructorCreatesTable() {
    // The K8sContext constructor delegates to the K8sApi constructor
    NamespaceTable contextTable = new NamespaceTable((K8sContext) null);
    assertNotNull(contextTable);
  }
}

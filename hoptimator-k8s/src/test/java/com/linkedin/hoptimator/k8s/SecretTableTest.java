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

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class SecretTableTest {

  @Mock
  private K8sApi<V1Secret, V1SecretList> mockApi;

  private SecretTable table;

  @BeforeEach
  void setUp() {
    table = new SecretTable(mockApi);
  }

  @Test
  void toRowMapsNameAndType() {
    V1Secret secret = new V1Secret()
        .metadata(new V1ObjectMeta().name("my-secret"))
        .type("Opaque");

    SecretTable.Row row = table.toRow(secret);

    assertEquals("my-secret", row.NAME);
    assertEquals("Opaque", row.TYPE);
  }

  @Test
  void getElementTypeReturnsRowClass() {
    assertEquals(SecretTable.Row.class, table.getElementType());
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
    V1Secret secret1 = new V1Secret()
        .metadata(new V1ObjectMeta().name("secret1"))
        .type("Opaque");
    V1Secret secret2 = new V1Secret()
        .metadata(new V1ObjectMeta().name("secret2"))
        .type("kubernetes.io/tls");

    when(mockApi.list()).thenReturn(Arrays.asList(secret1, secret2));

    Collection<SecretTable.Row> rows = table.rows();

    assertEquals(2, rows.size());
  }

  @Test
  void apiReturnsProvidedApi() {
    assertSame(mockApi, table.api());
  }

  @Test
  void fromRowThrowsUnsupportedOperation() {
    SecretTable.Row row = new SecretTable.Row("secret", "Opaque");
    assertThrows(UnsupportedOperationException.class, () -> table.fromRow(row));
  }

  @Test
  void getModifiableCollectionReturnsSameAsRows() {
    assertSame(table.rows(), table.getModifiableCollection());
  }

  @Test
  void contextConstructorCreatesTable() {
    // The K8sContext constructor delegates to the K8sApi constructor
    SecretTable contextTable = new SecretTable((K8sContext) null);
    assertNotNull(contextTable);
  }
}

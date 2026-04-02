package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class WrappedTest {

  @Mock
  private HoptimatorConnection mockConnection;

  @Mock
  private SchemaPlus mockSchema;

  private Wrapped wrapped;

  @BeforeEach
  void setUp() {
    wrapped = new Wrapped(mockConnection, mockSchema);
  }

  @Test
  void testConnectionReturnsInjectedConnection() {
    assertEquals(mockConnection, wrapped.connection());
  }

  @Test
  void testSchemaReturnsInjectedSchema() {
    assertEquals(mockSchema, wrapped.schema());
  }

  @Test
  void testUnwrapReturnsConnectionForConnectionClass() throws Exception {
    HoptimatorConnection result = wrapped.unwrap(HoptimatorConnection.class);
    assertEquals(mockConnection, result);
  }

  @Test
  void testUnwrapReturnsSchemaForSchemaClass() throws Exception {
    SchemaPlus result = wrapped.unwrap(SchemaPlus.class);
    assertEquals(mockSchema, result);
  }

  @Test
  void testUnwrapReturnsNullForUnknownClass() throws Exception {
    String result = wrapped.unwrap(String.class);
    assertNull(result);
  }

  @Test
  void testIsWrapperForSchemaPlus() {
    assertTrue(wrapped.isWrapperFor(SchemaPlus.class));
  }

  @Test
  void testIsWrapperForHoptimatorConnection() {
    assertTrue(wrapped.isWrapperFor(HoptimatorConnection.class));
  }

  @Test
  void testIsWrapperForUnknownClass() {
    assertFalse(wrapped.isWrapperFor(String.class));
  }
}

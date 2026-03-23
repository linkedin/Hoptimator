package com.linkedin.hoptimator.util;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class ApiTest {

  @Test
  void testDefaultCreateThrowsUnsupportedOperation() {
    Api<String> readOnlyApi = () -> Arrays.asList("a", "b");

    UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
        () -> readOnlyApi.create("c"));
    assertEquals("This API is read-only.", ex.getMessage());
  }

  @Test
  void testDefaultDeleteThrowsUnsupportedOperation() {
    Api<String> readOnlyApi = () -> Arrays.asList("a", "b");

    UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
        () -> readOnlyApi.delete("a"));
    assertEquals("This API is read-only.", ex.getMessage());
  }

  @Test
  void testDefaultUpdateThrowsUnsupportedOperation() {
    Api<String> readOnlyApi = () -> Arrays.asList("a", "b");

    UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
        () -> readOnlyApi.update("a"));
    assertEquals("This API is read-only.", ex.getMessage());
  }

  @Test
  void testListReturnsItems() throws SQLException {
    Api<String> api = () -> Arrays.asList("x", "y", "z");

    Collection<String> result = api.list();

    assertEquals(3, result.size());
  }
}

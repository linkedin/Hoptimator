package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class ViewTest {

  @Test
  void testTableReturnsLastPathElement() {
    View view = new View(List.of("schema", "myView"), "SELECT 1");
    assertEquals("myView", view.table());
  }

  @Test
  void testSchemaReturnsPenultimateElement() {
    View view = new View(List.of("catalog", "schema", "myView"), "SELECT 1");
    assertEquals("schema", view.schema());
  }

  @Test
  void testSchemaReturnsNullForSingleElementPath() {
    View view = new View(List.of("myView"), "SELECT 1");
    assertNull(view.schema());
  }

  @Test
  void testCatalogReturnsThirdFromEnd() {
    View view = new View(List.of("catalog", "schema", "myView"), "SELECT 1");
    assertEquals("catalog", view.catalog());
  }

  @Test
  void testCatalogReturnsNullForTwoElementPath() {
    View view = new View(List.of("schema", "myView"), "SELECT 1");
    assertNull(view.catalog());
  }

  @Test
  void testViewSql() {
    View view = new View(List.of("v"), "SELECT * FROM t");
    assertEquals("SELECT * FROM t", view.viewSql());
  }

  @Test
  void testPath() {
    List<String> path = List.of("a", "b");
    View view = new View(path, "SELECT 1");
    assertEquals(path, view.path());
  }

  @Test
  void testToString() {
    View view = new View(List.of("s", "v"), "SELECT 1");
    assertEquals("View[s.v]", view.toString());
  }
}

package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class SourceTest {

  @Test
  void testTableReturnsLastPathElement() {
    Source source = new Source("db", List.of("catalog", "schema", "myTable"), Collections.emptyMap());
    assertEquals("myTable", source.table());
  }

  @Test
  void testSchemaReturnsPenultimateElement() {
    Source source = new Source("db", List.of("catalog", "schema", "myTable"), Collections.emptyMap());
    assertEquals("schema", source.schema());
  }

  @Test
  void testCatalogReturnsThirdFromEnd() {
    Source source = new Source("db", List.of("catalog", "schema", "myTable"), Collections.emptyMap());
    assertEquals("catalog", source.catalog());
  }

  @Test
  void testCatalogReturnsNullForTwoElementPath() {
    Source source = new Source("db", List.of("schema", "myTable"), Collections.emptyMap());
    assertNull(source.catalog());
  }

  @Test
  void testDatabase() {
    Source source = new Source("myDb", List.of("t"), Collections.emptyMap());
    assertEquals("myDb", source.database());
  }

  @Test
  void testOptions() {
    Map<String, String> opts = Map.of("key", "value");
    Source source = new Source("db", List.of("t"), opts);
    assertEquals(opts, source.options());
  }

  @Test
  void testPath() {
    List<String> path = List.of("a", "b", "c");
    Source source = new Source("db", path, Collections.emptyMap());
    assertEquals(path, source.path());
  }

  @Test
  void testPathString() {
    Source source = new Source("db", List.of("a", "b", "c"), Collections.emptyMap());
    assertEquals("a.b.c", source.pathString());
  }

  @Test
  void testToString() {
    Source source = new Source("db", List.of("a", "b"), Collections.emptyMap());
    assertEquals("Source[a.b]", source.toString());
  }

  @Test
  void testSchemaReturnsValueForExactlyTwoElementPath() {
    Source source = new Source("db", List.of("mySchema", "myTable"), Collections.emptyMap());
    assertEquals("mySchema", source.schema(),
        "schema() must return the second-to-last element when path has exactly 2 elements");
  }

  @Test
  void testSchemaReturnsNullForExactlyOneElementPath() {
    Source source = new Source("db", List.of("onlyTable"), Collections.emptyMap());
    assertNull(source.schema(),
        "schema() must return null when path has only 1 element");
  }
}

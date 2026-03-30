package com.linkedin.hoptimator.k8s;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sViewTableRowTest {

  @Test
  void viewPathWithAllFields() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", "catalog", "schema", "view", "SELECT 1", false);
    List<String> path = row.viewPath();
    assertEquals(3, path.size());
    assertEquals("catalog", path.get(0));
    assertEquals("schema", path.get(1));
    assertEquals("view", path.get(2));
  }

  @Test
  void viewPathWithNullCatalog() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", null, "schema", "view", "SELECT 1", false);
    List<String> path = row.viewPath();
    assertEquals(2, path.size());
    assertEquals("schema", path.get(0));
    assertEquals("view", path.get(1));
  }

  @Test
  void viewPathWithNullSchema() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", "catalog", null, "view", "SELECT 1", false);
    List<String> path = row.viewPath();
    assertEquals(3, path.size());
    assertEquals("catalog", path.get(0));
    assertEquals("DEFAULT", path.get(1));
    assertEquals("view", path.get(2));
  }

  @Test
  void schemaPathWithBothCatalogAndSchema() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", "cat", "sch", "v", "SELECT 1", false);
    List<String> path = row.schemaPath();
    assertEquals(2, path.size());
    assertEquals("cat", path.get(0));
    assertEquals("sch", path.get(1));
  }

  @Test
  void schemaPathWithNullCatalogAndNullSchema() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", null, null, "v", "SELECT 1", false);
    List<String> path = row.schemaPath();
    assertEquals(1, path.size());
    assertEquals("DEFAULT", path.get(0));
  }

  @Test
  void viewNameReturnsViewWhenNotNull() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", null, null, "my-view", "SELECT 1", false);
    assertEquals("my-view", row.viewName());
  }

  @Test
  void viewNameReturnsNameWhenViewIsNull() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", null, null, null, "SELECT 1", false);
    assertEquals("name", row.viewName());
  }

  @Test
  void toStringContainsAllFields() {
    K8sViewTable.Row row = new K8sViewTable.Row("name", "cat", "sch", "view", "SELECT 1", true);
    String str = row.toString();
    assertTrue(str.contains("name"));
    assertTrue(str.contains("cat"));
    assertTrue(str.contains("sch"));
    assertTrue(str.contains("view"));
    assertTrue(str.contains("SELECT 1"));
    assertTrue(str.contains("true"));
  }
}

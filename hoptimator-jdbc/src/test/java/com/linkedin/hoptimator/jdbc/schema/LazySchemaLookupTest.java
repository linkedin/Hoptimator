package com.linkedin.hoptimator.jdbc.schema;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LazySchemaLookupTest {

  private static class MockSchema extends AbstractSchema {
    private final String name;

    MockSchema(String name) {
      this.name = name;
    }

    String getName() {
      return name;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
      return this;
    }
  }

  private static class SimpleSchemaLookup extends LazySchemaLookup<MockSchema> {
    private final Map<String, MockSchema> schemas;
    final AtomicInteger loadAllSchemasCallCount = new AtomicInteger(0);
    final AtomicInteger loadSchemaCallCount = new AtomicInteger(0);

    SimpleSchemaLookup(String... schemaNames) {
      this.schemas = new HashMap<>();
      for (String name : schemaNames) {
        schemas.put(name, new MockSchema(name));
      }
    }

    @Override
    protected Map<String, MockSchema> loadAllSchemas() {
      loadAllSchemasCallCount.incrementAndGet();
      return new HashMap<>(schemas);
    }

    @Override
    protected @Nullable MockSchema loadSchema(String name) {
      loadSchemaCallCount.incrementAndGet();
      return schemas.get(name);
    }

    @Override
    protected String getCatalogDescription() {
      return "Test Catalog";
    }
  }

  @Test
  void testGetNull() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1", "db2");
    assertNull(lookup.get("unknown"));
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testGet() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1", "db2");
    MockSchema schema = lookup.get("db1");
    assertNotNull(schema);
    assertEquals("db1", schema.getName());
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testGetCached() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1");
    MockSchema first = lookup.get("db1");
    MockSchema second = lookup.get("db1");
    assertSame(first, second);
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testGetNamesAny() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1", "db2", "db3");
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(3, names.size());
    assertTrue(names.contains("db1"));
    assertTrue(names.contains("db2"));
    assertTrue(names.contains("db3"));
    assertEquals(1, lookup.loadAllSchemasCallCount.get());
    assertEquals(0, lookup.loadSchemaCallCount.get());
  }

  @Test
  void testGetNamesAlphanumeric() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1", "db2");
    Set<String> names = lookup.getNames(new LikePattern("db1"));
    assertEquals(1, names.size());
    assertTrue(names.contains("db1"));
    // Alphanumeric pattern uses get() optimization
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testGetNamesAlphanumericNotFound() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1");
    Set<String> names = lookup.getNames(new LikePattern("missing"));
    assertEquals(0, names.size());
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testGetNamesWithWildcard() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("prod_db", "prod_archive", "staging");
    Set<String> names = lookup.getNames(new LikePattern("prod%"));
    assertEquals(2, names.size());
    assertTrue(names.contains("prod_db"));
    assertTrue(names.contains("prod_archive"));
    assertEquals(1, lookup.loadAllSchemasCallCount.get());
    assertEquals(0, lookup.loadSchemaCallCount.get());
  }

  @Test
  void testGetThenGetNames() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1", "db2");

    MockSchema schema = lookup.get("db1");
    assertNotNull(schema);
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());

    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertEquals(1, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testGetNamesThenGet() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1", "db2");

    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertEquals(1, lookup.loadAllSchemasCallCount.get());
    assertEquals(0, lookup.loadSchemaCallCount.get());

    // After loading all schemas, get() should use the cache, not loadSchema()
    MockSchema schema = lookup.get("db1");
    assertNotNull(schema);
    assertEquals(0, lookup.loadSchemaCallCount.get());
    assertEquals(1, lookup.loadAllSchemasCallCount.get());
  }

  @Test
  void testLoadSchemaException() {
    LazySchemaLookup<MockSchema> lookup = new LazySchemaLookup<>() {
      @Override
      protected Map<String, MockSchema> loadAllSchemas() {
        return new HashMap<>();
      }

      @Override
      protected @Nullable MockSchema loadSchema(String name) throws Exception {
        throw new RuntimeException("Simulated failure");
      }

      @Override
      protected String getCatalogDescription() {
        return "Test Catalog";
      }
    };

    RuntimeException exception = assertThrows(RuntimeException.class, () -> lookup.get("db1"));
    assertTrue(exception.getMessage().contains("Failed to load schema 'db1' from Test Catalog"),
        "Expected error message but got: " + exception.getMessage());
  }

  @Test
  void testLoadAllSchemasException() {
    LazySchemaLookup<MockSchema> lookup = new LazySchemaLookup<>() {
      @Override
      protected Map<String, MockSchema> loadAllSchemas() throws Exception {
        throw new RuntimeException("Simulated failure");
      }

      @Override
      protected @Nullable MockSchema loadSchema(String name) {
        return null;
      }

      @Override
      protected String getCatalogDescription() {
        return "Test Catalog";
      }
    };

    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> lookup.getNames(LikePattern.any()));
    assertTrue(exception.getMessage().contains("Failed to load schemas from Test Catalog"));
  }

  @Test
  void testGetNonExistentSchemaReturnsNull() {
    SimpleSchemaLookup lookup = new SimpleSchemaLookup("db1");
    MockSchema result = lookup.get("nonExistentDb");
    assertNull(result);
    assertEquals(1, lookup.loadSchemaCallCount.get());
    assertEquals(0, lookup.loadAllSchemasCallCount.get());
  }
}

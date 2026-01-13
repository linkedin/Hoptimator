package com.linkedin.hoptimator.jdbc.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Named;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LazyTableLookupTest {

  /**
   * Simple mock table for testing.
   */
  private static class MockTable extends AbstractTable {
    private final String name;

    MockTable(String name) {
      this.name = name;
    }

    String getName() {
      return name;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().build();
    }
  }

  /**
   * Simple test implementation that tracks load calls.
   */
  private static class SimpleLazyTableLookup extends LazyTableLookup<MockTable> {
    private final Map<String, MockTable> tables;
    final AtomicInteger loadAllTablesCallCount = new AtomicInteger(0);
    final AtomicInteger loadTableCallCount = new AtomicInteger(0);

    SimpleLazyTableLookup(String... tableNames) {
      this.tables = new HashMap<>();
      for (String name : tableNames) {
        tables.put(name, new MockTable(name));
      }
    }

    @Override
    protected Map<String, MockTable> loadAllTables() {
      loadAllTablesCallCount.incrementAndGet();
      return new HashMap<>(tables);
    }

    @Override
    protected @Nullable MockTable loadTable(String name) {
      loadTableCallCount.incrementAndGet();
      return tables.get(name);
    }

    @Override
    protected String getSchemaDescription() {
      return "Test Schema";
    }
  }

  // Basic get() tests

  @Test
  void testGetNull() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo");
    assertNull(lookup.get("unknown"));
    assertEquals(1, lookup.loadTableCallCount.get());
    assertEquals(0, lookup.loadAllTablesCallCount.get());
  }

  @Test
  void testGet() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo");
    MockTable table = lookup.get("test");
    assertNotNull(table);
    assertEquals("test", table.getName());
    assertEquals(1, lookup.loadTableCallCount.get());
    assertEquals(0, lookup.loadAllTablesCallCount.get());
  }

  @Test
  void testGetCached() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test");
    MockTable table1 = lookup.get("test");
    MockTable table2 = lookup.get("test");
    assertSame(table1, table2);
    assertEquals(1, lookup.loadTableCallCount.get()); // Only loaded once
    assertEquals(0, lookup.loadAllTablesCallCount.get());
  }

  // getIgnoreCase() tests

  @Test
  void testGetIgnoreCase() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo");
    Named<MockTable> result = lookup.getIgnoreCase("TEST");
    assertNotNull(result);
    assertEquals("test", result.name());
    assertEquals("test", result.entity().getName());
    // getIgnoreCase() calls getNames() to build the name map, then calls get() which will already be in cache
    assertEquals(1, lookup.loadAllTablesCallCount.get());
    assertEquals(0, lookup.loadTableCallCount.get());
  }

  // getNames() tests

  @Test
  void testGetNamesAny() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo", "bar");
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(3, names.size());
    assertTrue(names.contains("test"));
    assertTrue(names.contains("foo"));
    assertTrue(names.contains("bar"));
    assertEquals(1, lookup.loadAllTablesCallCount.get());
    assertEquals(0, lookup.loadTableCallCount.get());
  }

  @Test
  void testGetNamesAlphanumeric() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo");
    Set<String> names = lookup.getNames(new LikePattern("test"));
    assertEquals(1, names.size());
    assertTrue(names.contains("test"));
    // Alphanumeric pattern uses get() optimization
    assertEquals(1, lookup.loadTableCallCount.get());
    assertEquals(0, lookup.loadAllTablesCallCount.get());
  }

  @Test
  void testGetNamesAlphanumericNotFound() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test");
    Set<String> names = lookup.getNames(new LikePattern("unknown"));
    assertEquals(0, names.size());
    assertEquals(1, lookup.loadTableCallCount.get());
    assertEquals(0, lookup.loadAllTablesCallCount.get());
  }

  @Test
  void testGetNamesWithWildcard() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test_one", "test_two", "other");
    Set<String> names = lookup.getNames(new LikePattern("test%"));
    assertEquals(2, names.size());
    assertTrue(names.contains("test_one"));
    assertTrue(names.contains("test_two"));
    // Wildcard pattern loads all tables
    assertEquals(1, lookup.loadAllTablesCallCount.get());
    assertEquals(0, lookup.loadTableCallCount.get());
  }

  // Mixed access patterns

  @Test
  void testGetThenGetNames() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo");

    // First get a specific table
    MockTable table = lookup.get("test");
    assertNotNull(table);
    assertEquals(1, lookup.loadTableCallCount.get());
    assertEquals(0, lookup.loadAllTablesCallCount.get());

    // Then get all names
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertEquals(1, lookup.loadTableCallCount.get()); // Not called again, used allTablesCache
    assertEquals(1, lookup.loadAllTablesCallCount.get());
  }

  @Test
  void testGetNamesThenGet() {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test", "foo");

    // First get all names
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertEquals(1, lookup.loadAllTablesCallCount.get());
    assertEquals(0, lookup.loadTableCallCount.get());

    // Then get specific table - should come from allTablesCache
    MockTable table = lookup.get("test");
    assertNotNull(table);
    assertEquals(0, lookup.loadTableCallCount.get()); // Not called, used allTablesCache
    assertEquals(1, lookup.loadAllTablesCallCount.get());
  }

  // Error handling

  @Test
  void testLoadTableException() {
    LazyTableLookup<MockTable> lookup = new LazyTableLookup<>() {
      @Override
      protected Map<String, MockTable> loadAllTables() {
        return new HashMap<>();
      }

      @Override
      protected @Nullable MockTable loadTable(String name) throws Exception {
        throw new RuntimeException("Simulated failure");
      }

      @Override
      protected String getSchemaDescription() {
        return "Test Schema";
      }
    };

    RuntimeException exception = assertThrows(RuntimeException.class, () -> lookup.get("test"));
    // The exception message should contain our error or be the simulated failure
    String message = exception.getMessage();
    assertTrue(message.contains("Failed to load table 'test' from Test Schema"),
        "Expected error message but got: " + message);
  }

  @Test
  void testLoadAllTablesException() {
    LazyTableLookup<MockTable> lookup = new LazyTableLookup<>() {
      @Override
      protected Map<String, MockTable> loadAllTables() throws Exception {
        throw new RuntimeException("Simulated failure");
      }

      @Override
      protected @Nullable MockTable loadTable(String name) {
        return null;
      }

      @Override
      protected String getSchemaDescription() {
        return "Test Schema";
      }
    };

    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> lookup.getNames(LikePattern.any()));
    assertTrue(exception.getMessage().contains("Failed to load tables from Test Schema"));
  }

  // Concurrent access

  @Test
  void testConcurrentGet() throws InterruptedException {
    SimpleLazyTableLookup lookup = new SimpleLazyTableLookup("test");

    int threadCount = 10;
    Set<MockTable> results = Collections.newSetFromMap(new ConcurrentHashMap<>());
    CountDownLatch latch = new CountDownLatch(threadCount);

    for (int i = 0; i < threadCount; i++) {
      new Thread(() -> {
        results.add(lookup.get("test"));
        latch.countDown();
      }).start();
    }

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals(1, results.size()); // All threads got the same instance
    // With computeIfAbsent, should only load once
    assertEquals(1, lookup.loadTableCallCount.get());
    assertEquals(0, lookup.loadAllTablesCallCount.get());
  }
}

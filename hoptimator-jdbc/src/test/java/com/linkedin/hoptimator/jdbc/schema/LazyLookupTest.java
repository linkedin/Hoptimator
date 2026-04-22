package com.linkedin.hoptimator.jdbc.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Named;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LazyLookupTest {

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
  private static class SimpleLazyLookup extends LazyLookup<MockTable> {
    private final Map<String, MockTable> tables;
    final AtomicInteger loadAllCallCount = new AtomicInteger(0);
    final AtomicInteger loadCallCount = new AtomicInteger(0);

    SimpleLazyLookup(String... tableNames) {
      this.tables = new HashMap<>();
      for (String name : tableNames) {
        tables.put(name, new MockTable(name));
      }
    }

    @Override
    protected Map<String, MockTable> loadAll() {
      loadAllCallCount.incrementAndGet();
      return new HashMap<>(tables);
    }

    @Override
    protected @Nullable MockTable load(String name) {
      loadCallCount.incrementAndGet();
      return tables.get(name);
    }

    @Override
    protected String getDescription() {
      return "Test Schema";
    }
  }

  // Basic get() tests

  @Test
  void testGetNull() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo");
    assertNull(lookup.get("unknown"));
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  @Test
  void testGet() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo");
    MockTable table = lookup.get("test");
    assertNotNull(table);
    assertEquals("test", table.getName());
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  @Test
  void testGetCached() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test");
    MockTable table1 = lookup.get("test");
    MockTable table2 = lookup.get("test");
    assertSame(table1, table2);
    assertEquals(1, lookup.loadCallCount.get()); // Only loaded once
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  // getIgnoreCase() tests

  @Test
  void testGetIgnoreCase() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo");
    Named<MockTable> result = lookup.getIgnoreCase("TEST");
    assertNotNull(result);
    assertEquals("test", result.name());
    assertEquals("test", result.entity().getName());
    // getIgnoreCase() calls getNames() to build the name map, then calls get() which will already be in cache
    assertEquals(1, lookup.loadAllCallCount.get());
    assertEquals(0, lookup.loadCallCount.get());
  }

  // getNames() tests

  @Test
  void testGetNamesAny() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo", "bar");
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(3, names.size());
    assertTrue(names.contains("test"));
    assertTrue(names.contains("foo"));
    assertTrue(names.contains("bar"));
    assertEquals(1, lookup.loadAllCallCount.get());
    assertEquals(0, lookup.loadCallCount.get());
  }

  @Test
  void testGetNamesAlphanumeric() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo");
    Set<String> names = lookup.getNames(new LikePattern("test"));
    assertEquals(1, names.size());
    assertTrue(names.contains("test"));
    // Alphanumeric pattern uses get() optimization
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  @Test
  void testGetNamesAlphanumericNotFound() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test");
    Set<String> names = lookup.getNames(new LikePattern("unknown"));
    assertEquals(0, names.size());
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  @Test
  void testGetNamesWithWildcard() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test_one", "test_two", "other");
    Set<String> names = lookup.getNames(new LikePattern("test%"));
    assertEquals(2, names.size());
    assertTrue(names.contains("test_one"));
    assertTrue(names.contains("test_two"));
    // Wildcard pattern loads all entries
    assertEquals(1, lookup.loadAllCallCount.get());
    assertEquals(0, lookup.loadCallCount.get());
  }

  // Mixed access patterns

  @Test
  void testGetThenGetNames() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo");

    // First get a specific entry
    MockTable table = lookup.get("test");
    assertNotNull(table);
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());

    // Then get all names
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertEquals(1, lookup.loadCallCount.get()); // Not called again, used allTablesCache
    assertEquals(1, lookup.loadAllCallCount.get());
  }

  @Test
  void testGetNamesThenGet() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test", "foo");

    // First get all names
    Set<String> names = lookup.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertEquals(1, lookup.loadAllCallCount.get());
    assertEquals(0, lookup.loadCallCount.get());

    // Then get specific entry - should come from allTablesCache
    MockTable table = lookup.get("test");
    assertNotNull(table);
    assertEquals(0, lookup.loadCallCount.get()); // Not called, used allTablesCache
    assertEquals(1, lookup.loadAllCallCount.get());
  }

  // Error handling

  @Test
  void testLoadException() {
    LazyLookup<MockTable> lookup = new LazyLookup<>() {
      @Override
      protected Map<String, MockTable> loadAll() {
        return new HashMap<>();
      }

      @Override
      protected @Nullable MockTable load(String name) throws Exception {
        throw new RuntimeException("Simulated failure");
      }

      @Override
      protected String getDescription() {
        return "Test Schema";
      }
    };

    RuntimeException exception = assertThrows(RuntimeException.class, () -> lookup.get("test"));
    String message = exception.getMessage();
    assertTrue(message.contains("Failed to load 'test' from Test Schema"),
        "Expected error message but got: " + message);
  }

  @Test
  void testLoadAllException() {
    LazyLookup<MockTable> lookup = new LazyLookup<>() {
      @Override
      protected Map<String, MockTable> loadAll() throws Exception {
        throw new RuntimeException("Simulated failure");
      }

      @Override
      protected @Nullable MockTable load(String name) {
        return null;
      }

      @Override
      protected String getDescription() {
        return "Test Schema";
      }
    };

    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> lookup.getNames(LikePattern.any()));
    assertTrue(exception.getMessage().contains("Failed to load entries from Test Schema"));
  }

  @Test
  void testGetNonExistentEntryReturnsNull() {
    SimpleLazyLookup lookup = new SimpleLazyLookup("table1", "table2");
    MockTable result = lookup.get("nonExistentTable");
    // Must be null, not a non-null placeholder
    assertNull(result);
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  @Test
  void testGetNamesSimpleAlphanumericPatternUsesGetNotLoadAll() {
    // Ensures alphanumeric fast-path
    SimpleLazyLookup lookup = new SimpleLazyLookup("PRINT", "OTHER");
    Set<String> names = lookup.getNames(new LikePattern("PRINT"));
    assertEquals(1, names.size());
    assertTrue(names.contains("PRINT"));
    // Fast path must call load(), NOT loadAll()
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  @Test
  void testGetNamesWildcardPatternCallsLoadAll() {
    // Contrasts with simple pattern — ensures the branch is actually taken differently
    SimpleLazyLookup lookup = new SimpleLazyLookup("PRINT", "OTHER");
    Set<String> names = lookup.getNames(new LikePattern("%"));
    assertEquals(2, names.size());
    // Wildcard pattern must call loadAll(), NOT load()
    assertEquals(1, lookup.loadAllCallCount.get());
    assertEquals(0, lookup.loadCallCount.get());
  }

  @Test
  void testGetNamesSimplePatternForMissingEntryReturnsEmpty() {
    // If alphanumeric fast-path is removed, missing entry would trigger loadAll
    SimpleLazyLookup lookup = new SimpleLazyLookup("PRINT");
    Set<String> names = lookup.getNames(new LikePattern("MISSING"));
    assertTrue(names.isEmpty());
    // Must use load() (fast path), not loadAll()
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }

  // Concurrent access

  @Test
  void testConcurrentGet() throws InterruptedException {
    SimpleLazyLookup lookup = new SimpleLazyLookup("test");

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
    assertEquals(1, lookup.loadCallCount.get());
    assertEquals(0, lookup.loadAllCallCount.get());
  }
}

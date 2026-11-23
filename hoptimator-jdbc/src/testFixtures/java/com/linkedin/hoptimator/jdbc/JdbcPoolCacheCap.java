package com.linkedin.hoptimator.jdbc;

import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;


/**
 * Test-only cache cap for an isolated process. Call {@link #enforce()} after each operation.
 * Forced eviction can remove an active catalog's entry, unlike soft-reference reclamation.
 */
public final class JdbcPoolCacheCap {
  private final JdbcDataSourcePool owner;
  private final Map<?, ?> pools;
  private final int capacity;
  private final Set<Object> keys = new LinkedHashSet<>();

  public JdbcPoolCacheCap(JdbcDataSourcePool owner, int capacity) throws ReflectiveOperationException {
    if (capacity < 0) {
      throw new IllegalArgumentException("Cache capacity must not be negative");
    }
    this.owner = owner;
    this.capacity = capacity;
    Field field = JdbcDataSourcePool.class.getDeclaredField("pools");
    field.setAccessible(true);
    this.pools = (Map<?, ?>) field.get(owner);
  }

  public synchronized int enforce() {
    synchronized (owner) {
      keys.retainAll(pools.keySet());
      keys.addAll(pools.keySet());
      Iterator<Object> iterator = keys.iterator();
      while (pools.size() > capacity) {
        Object entry = pools.remove(iterator.next());
        iterator.remove();
        if (!(entry instanceof Reference<?>)) {
          throw new IllegalStateException("Expected a cached soft reference");
        }
        // The Cleaner also retains the reference, so removing the map entry alone is insufficient.
        ((Reference<?>) entry).clear();
      }
      return pools.size();
    }
  }
}

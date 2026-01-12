package com.linkedin.hoptimator.jdbc.schema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A lazy-loading table lookup implementation that optimizes table discovery.
 *
 * <p>This class provides efficient lazy-loading behavior for schema tables:
 * <ul>
 *   <li>When {@code get(name)} is called, only that specific table is loaded</li>
 *   <li>When {@code getNames(pattern)} is called with a simple pattern (no wildcards),
 *       only matching tables are loaded</li>
 *   <li>When {@code getNames(pattern)} is called with wildcards (%), all tables are loaded</li>
 * </ul>
 *
 * <p>This avoids the performance penalty of loading all tables when only a subset is needed.
 *
 * @param <T> The type of table being looked up (typically {@link Table})
 */
public abstract class LazyTableLookup<T extends Table> extends IgnoreCaseLookup<T> {

  private static final Logger log = LoggerFactory.getLogger(LazyTableLookup.class);

  // TODO: consider replacing with an eviction cache via LoadingCache as LoadingCacheLookup does
  private final ConcurrentHashMap<String, T> tableCache = new ConcurrentHashMap<>();
  private volatile Map<String, T> allTablesCache = null;
  private final Object allTablesLock = new Object();

  /**
   * Load all tables from the underlying data source.
   * This method is called only when necessary (e.g., when a wildcard pattern is used).
   *
   * @return A map of all tables keyed by name
   * @throws Exception if tables cannot be loaded
   */
  protected abstract Map<String, T> loadAllTables() throws Exception;

  /**
   * Load a specific table by name from the underlying data source.
   * This method is called on-demand when a specific table is requested.
   *
   * @param name The name of the table to load
   * @return The table instance, or null if the table doesn't exist
   * @throws Exception if the table cannot be loaded
   */
  @Nullable
  protected abstract T loadTable(String name) throws Exception;

  /**
   * Get the name to use for logging/error messages.
   *
   * @return A descriptive name for this schema (e.g., "Kafka cluster", "MySQL database")
   */
  protected abstract String getSchemaDescription();

  @Override
  public @Nullable T get(String name) {
    // Check if we already have this table cached
    T cached = tableCache.get(name);
    if (cached != null) {
      return cached;
    }

    // Check if we've already loaded all tables
    if (allTablesCache != null) {
      return allTablesCache.get(name);
    }

    // Load this specific table with proper synchronization to avoid duplicate loads
    return tableCache.computeIfAbsent(name, key -> {
      try {
        log.debug("Loading table '{}' from {}", key, getSchemaDescription());
        return loadTable(key);
      } catch (Exception e) {
        String errorMessage = String.format("Failed to load table '%s' from %s", key, getSchemaDescription());
        log.error(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    });
  }

  @Override
  public Set<String> getNames(LikePattern pattern) {
    // Check if the pattern requires loading all tables
    // For some reason internals in Calcite often call this method with a pattern that matches a single table
    if (pattern.pattern.matches("[A-Za-z0-9-_]+")) {
      T table = get(pattern.pattern);
      return table != null
          ? Collections.singleton(pattern.pattern)
          : Collections.emptySet();
    }

    // Load all tables only when required. This could likely be optimized more depending on the type
    // of regex used
    try {
      if (allTablesCache == null) {
        synchronized (allTablesLock) {
          if (allTablesCache == null) {
            log.info("Loading all tables from {}", getSchemaDescription());
            allTablesCache = loadAllTables();
            log.info("Loaded {} tables.", allTablesCache.size());
          }
        }
      }
      return allTablesCache.keySet()
          .stream()
          .filter(tableName -> pattern.matcher().apply(tableName))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      String errorMessage = String.format("Failed to load tables from %s", getSchemaDescription());
      log.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}

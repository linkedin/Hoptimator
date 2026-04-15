package com.linkedin.hoptimator.jdbc.schema;

import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * A lazy-loading lookup implementation that optimizes entity discovery.
 *
 * <p>This class provides efficient lazy-loading behavior for named entities (tables, schemas):
 * <ul>
 *   <li>When {@code get(name)} is called, only that specific entry is loaded</li>
 *   <li>When {@code getNames(pattern)} is called with a simple pattern (no wildcards),
 *       only matching entries are loaded</li>
 *   <li>When {@code getNames(pattern)} is called with wildcards (%), all entries are loaded</li>
 * </ul>
 *
 * <p>This avoids the performance penalty of loading all entries when only a subset is needed.
 *
 * @param <T> The type of entity being looked up (e.g. {@code Table}, {@code Schema})
 */
public abstract class LazyLookup<T> extends IgnoreCaseLookup<T> {

  private static final Logger log = LoggerFactory.getLogger(LazyLookup.class);

  // TODO: consider replacing with an eviction cache via LoadingCache as LoadingCacheLookup does
  private final ConcurrentHashMap<String, T> tableCache = new ConcurrentHashMap<>();
  private volatile Map<String, T> allTablesCache = null;
  private final Object allTablesLock = new Object();

  /**
   * Load all entries from the underlying data source.
   * This method is called only when necessary (e.g., when a wildcard pattern is used).
   *
   * @return A map of all entries keyed by name
   * @throws Exception if entries cannot be loaded
   */
  protected abstract Map<String, T> loadAll() throws Exception;

  /**
   * Load a specific entry by name from the underlying data source.
   * This method is called on-demand when a specific entry is requested.
   *
   * @param name The name of the entry to load
   * @return The entry instance, or null if it doesn't exist
   * @throws Exception if the entry cannot be loaded
   */
  @Nullable
  protected abstract T load(String name) throws Exception;

  /**
   * Get the name to use for logging/error messages.
   *
   * @return A descriptive name for this source (e.g., "Kafka cluster", "MySQL database")
   */
  protected abstract String getDescription();

  @Override
  public @Nullable T get(String name) {
    // Check if we already have this entry cached
    T cached = tableCache.get(name);
    if (cached != null) {
      return cached;
    }

    // Check if we've already loaded all entries
    if (allTablesCache != null) {
      return allTablesCache.get(name);
    }

    // Load this specific entry with proper synchronization to avoid duplicate loads
    return tableCache.computeIfAbsent(name, key -> {
      try {
        log.debug("Loading '{}' from {}", key, getDescription());
        return load(key);
      } catch (Exception e) {
        String errorMessage = String.format("Failed to load '%s' from %s", key, getDescription());
        log.error(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    });
  }

  @Override
  public Set<String> getNames(LikePattern pattern) {
    // Check if the pattern requires loading all entries
    // For some reason internals in Calcite often call this method with a pattern that matches a single entry
    if (pattern.pattern.matches("[A-Za-z0-9-_]+")) {
      T entry = get(pattern.pattern);
      return entry != null
          ? Collections.singleton(pattern.pattern)
          : Collections.emptySet();
    }

    // Load all entries only when required. This could likely be optimized more depending on the type
    // of regex used
    try {
      if (allTablesCache == null) {
        synchronized (allTablesLock) {
          if (allTablesCache == null) {
            log.info("Loading all entries from {}", getDescription());
            allTablesCache = loadAll();
            log.info("Loaded {} entries.", allTablesCache.size());
          }
        }
      }
      return allTablesCache.keySet()
          .stream()
          .filter(entryName -> pattern.matcher().apply(entryName))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      String errorMessage = String.format("Failed to load entries from %s", getDescription());
      log.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}

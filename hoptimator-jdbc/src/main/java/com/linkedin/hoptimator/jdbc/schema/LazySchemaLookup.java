package com.linkedin.hoptimator.jdbc.schema;

import org.apache.calcite.schema.Schema;
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
 * A lazy-loading schema lookup that defers schema discovery until access time.
 *
 * <p>This class provides efficient lazy-loading behavior for catalog sub-schemas:
 * <ul>
 *   <li>When {@code get(name)} is called, only that specific schema is loaded</li>
 *   <li>When {@code getNames(pattern)} is called with a simple pattern (no wildcards),
 *       only matching schemas are loaded</li>
 *   <li>When {@code getNames(pattern)} is called with wildcards (%), all schemas are loaded</li>
 * </ul>
 *
 * <p>This avoids the performance penalty of loading all schemas at driver connection time,
 * following the same pattern as {@link LazyTableLookup} for tables.
 *
 * @param <S> The type of schema being looked up (typically {@link Schema})
 */
public abstract class LazySchemaLookup<S extends Schema> extends IgnoreCaseLookup<S> {

  private static final Logger log = LoggerFactory.getLogger(LazySchemaLookup.class);

  private final ConcurrentHashMap<String, S> schemaCache = new ConcurrentHashMap<>();
  private volatile Map<String, S> allSchemasCache = null;
  private final Object allSchemasLock = new Object();

  /**
   * Load all schemas from the underlying data source.
   * This method is called only when necessary (e.g., when a wildcard pattern is used).
   *
   * @return A map of all schemas keyed by name
   * @throws Exception if schemas cannot be loaded
   */
  protected abstract Map<String, S> loadAllSchemas() throws Exception;

  /**
   * Load a specific schema by name from the underlying data source.
   * This method is called on-demand when a specific schema is requested.
   *
   * @param name The name of the schema to load
   * @return The schema instance, or null if the schema doesn't exist
   * @throws Exception if the schema cannot be loaded
   */
  @Nullable
  protected abstract S loadSchema(String name) throws Exception;

  /**
   * Get the name to use for logging/error messages.
   *
   * @return A descriptive name for this catalog (e.g., "MySQL at jdbc:mysql://host:3306")
   */
  protected abstract String getCatalogDescription();

  @Override
  public @Nullable S get(String name) {
    // Check if we already have this schema cached
    S cached = schemaCache.get(name);
    if (cached != null) {
      return cached;
    }

    // Check if we've already loaded all schemas
    if (allSchemasCache != null) {
      return allSchemasCache.get(name);
    }

    // Load this specific schema with proper synchronization to avoid duplicate loads
    return schemaCache.computeIfAbsent(name, key -> {
      try {
        log.debug("Loading schema '{}' from {}", key, getCatalogDescription());
        return loadSchema(key);
      } catch (Exception e) {
        String errorMessage = String.format("Failed to load schema '%s' from %s", key, getCatalogDescription());
        log.error(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    });
  }

  @Override
  public Set<String> getNames(LikePattern pattern) {
    // Check if the pattern requires loading all schemas
    // For some reason internals in Calcite often call this method with a pattern that matches a single schema
    if (pattern.pattern.matches("[A-Za-z0-9-_]+")) {
      S schema = get(pattern.pattern);
      return schema != null
          ? Collections.singleton(pattern.pattern)
          : Collections.emptySet();
    }

    // Load all schemas only when required
    try {
      if (allSchemasCache == null) {
        synchronized (allSchemasLock) {
          if (allSchemasCache == null) {
            log.info("Loading all schemas from {}", getCatalogDescription());
            allSchemasCache = loadAllSchemas();
            log.info("Loaded {} schemas.", allSchemasCache.size());
          }
        }
      }
      return allSchemasCache.keySet()
          .stream()
          .filter(schemaName -> pattern.matcher().apply(schemaName))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      String errorMessage = String.format("Failed to load schemas from %s", getCatalogDescription());
      log.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}

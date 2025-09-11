package com.linkedin.hoptimator;

import java.util.Map;
import java.util.Set;


/**
 * Represents a data pipeline job with lazy-evaluated template functions.
 *
 * <p>A Job encapsulates the configuration and execution logic for a data pipeline,
 * including the destination sink and a collection of lazy-evaluated functions that
 * generate SQL scripts, field mappings, and other template values on demand.
 */
public class Job implements Deployable {

  private final String name;
  private final Set<Source> sources;
  private final Sink sink;

  /**
   * Lazy-evaluated template functions that generate various outputs for the job.
   *
   * <p>This map contains functions that are evaluated on-demand when specific
   * template values are needed. Each function takes a {@link SqlDialect} parameter
   * and returns a string representation of the requested output.
   *
   * @see ThrowingFunction
   * @see SqlDialect
   */
  private final Map<String, ThrowingFunction<SqlDialect, String>> lazyEvals;

  public Job(String name, Set<Source> sources, Sink sink, Map<String, ThrowingFunction<SqlDialect, String>> lazyEvals) {
    this.name = name;
    this.sources = sources;
    this.sink = sink;
    this.lazyEvals = lazyEvals;
  }

  public String name() {
    return name;
  }

  public Set<Source> sources() {
    return sources;
  }

  public Sink sink() {
    return sink;
  }

  /**
   * Retrieves a lazy-evaluated template function by key.
   *
   * <p>This method provides access to the template functions stored in {@link #lazyEvals}.
   * The returned function can be called with a {@link SqlDialect} to generate the
   * corresponding output string.
   *
   * <p><strong>Available Keys:</strong>
   * <ul>
   *   <li><code>"sql"</code> - Complete SQL script with INSERT INTO statements</li>
   *   <li><code>"query"</code> - SELECT query portion only</li>
   *   <li><code>"fieldMap"</code> - JSON mapping of source to destination fields</li>
   * </ul>
   *
   * @param key The template function key to retrieve
   * @return The lazy-evaluated function, or {@code null} if the key doesn't exist
   *
   * @see #lazyEvals
   * @see ThrowingFunction#apply(Object)
   */
  public ThrowingFunction<SqlDialect, String> eval(String key) {
    if (!lazyEvals.containsKey(key)) {
      throw new IllegalArgumentException("Unknown eval key: " + key + ". Available keys: " + lazyEvals.keySet());
    }
    return lazyEvals.get(key);
  }

  @Override
  public String toString() {
    return "Job[" + sink.pathString() + "]";
  }
}

package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.avro.Schema;

public class Source implements Deployable {

  private final String database;
  private final List<String> path;
  private final Map<String, String> options;
  private final @Nullable Schema rowSchema;

  public Source(String database, List<String> path, Map<String, String> options) {
    this(database, path, options, null);
  }

  public Source(String database, List<String> path, Map<String, String> options, @Nullable Schema rowSchema) {
    this.database = database;
    this.path = path;
    this.options = options;
    this.rowSchema = rowSchema;
  }

  public Map<String, String> options() {
    return options;
  }

  /**
   * The resolved row schema (Avro) of this table, if known. Populated by the producer that
   * built this deployable (the SQL planner or a direct API call). May be {@code null} when the
   * schema is not yet resolved, in which case consumers fall back to resolving it from the
   * catalog. This is how a deployable carries its own schema instead of it being looked up by
   * name from a Calcite connection.
   */
  public @Nullable Schema rowSchema() {
    return rowSchema;
  }

  /** The internal name for the database this table belongs to. Not necessary the same as schema. */
  public String database() {
    return database;
  }

  public String table() {
    return path.get(path.size() - 1);
  }

  /**
   * Returns the schema name if present.
   */
  public String schema() {
    return path.size() >= 2 ? path.get(path.size() - 2) : null;
  }

  /**
   * Returns the catalog name if present (3-level path), or null for 2-level paths.
   */
  public String catalog() {
    return path.size() >= 3 ? path.get(path.size() - 3) : null;
  }

  public List<String> path() {
    return path;
  }

  public String pathString() {
    return String.join(".", path);
  }

  @Override
  public String toString() {
    return "Source[" + pathString() + "]";
  }
}

package com.linkedin.hoptimator.operator.trigger;

import java.util.Objects;


/**
 * Identifies the input a {@code TableTrigger} watches: an optional {@code catalog} (present only
 * for three-part identifiers, e.g. {@code OPENHOUSE}), a {@code schema}, and a {@code table}. This
 * is the handle an {@link InputWatermarkProvider} resolves to a data-time completeness watermark.
 */
public final class TriggerInput {
  private final String catalog;
  private final String schema;
  private final String table;

  public TriggerInput(String catalog, String schema, String table) {
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
  }

  /** Optional catalog (e.g. {@code OPENHOUSE}); null for two-part identifiers. */
  public String catalog() {
    return catalog;
  }

  public String schema() {
    return schema;
  }

  public String table() {
    return table;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TriggerInput)) {
      return false;
    }
    TriggerInput that = (TriggerInput) o;
    return Objects.equals(catalog, that.catalog)
        && Objects.equals(schema, that.schema)
        && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, schema, table);
  }

  @Override
  public String toString() {
    return (catalog == null ? "" : catalog + ".") + schema + "." + table;
  }
}

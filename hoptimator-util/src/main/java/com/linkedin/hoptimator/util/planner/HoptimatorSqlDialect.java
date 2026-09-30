package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.sql.dialect.AnsiSqlDialect;


/**
 * ANSI SQL dialect used when rendering generated pipeline SQL (DDL, INSERT ... SELECT, etc.).
 *
 * <p>This behaves like {@link AnsiSqlDialect#DEFAULT} except that it does <em>not</em> emit a
 * {@code CHARACTER SET} clause when unparsing casts to character types. Calcite's
 * {@code SqlDialect.getCastSpec} appends {@code CHARACTER SET <name>} whenever the target type
 * carries a charset (the type factory's default charset, {@code ISO-8859-1}) and the dialect's
 * {@code supportsCharSet()} returns {@code true}. That produces casts such as
 * {@code CAST('DELETED' AS VARCHAR CHARACTER SET `ISO-8859-1`)}.
 *
 * <p>When the generated SQL is re-parsed and validated by a downstream engine (e.g. Flink), its
 * own string literals use a different charset ({@code UTF-16LE}), and Calcite rejects the
 * cross-charset cast with "Cast function cannot convert value of type CHAR(n) CHARACTER SET
 * UTF-16LE to type VARCHAR CHARACTER SET ISO-8859-1". Suppressing the charset clause emits a
 * plain {@code CAST(... AS VARCHAR)}, which every engine accepts and which matches the SQL that
 * non-cast expressions (e.g. {@code TRIM(...)}/{@code CASE}) already produce.
 */
public final class HoptimatorSqlDialect {

  private HoptimatorSqlDialect() {
  }

  /** ANSI dialect that omits {@code CHARACTER SET} clauses from character-type casts. */
  public static final AnsiSqlDialect DEFAULT = new AnsiSqlDialect(AnsiSqlDialect.DEFAULT_CONTEXT) {
    @Override
    public boolean supportsCharSet() {
      return false;
    }
  };
}

package com.linkedin.hoptimator.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlConformanceEnum;


/** Helpers for turning user-typed SQL identifier strings into their component parts. */
public final class IdentifierUtils {

  private static final SqlParser.Config PARSER_CONFIG = SqlParser.config()
      .withQuoting(Quoting.DOUBLE_QUOTE)
      // Preserve case exactly as typed — Hoptimator schema/table names are case-sensitive.
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED)
      .withConformance(SqlConformanceEnum.BABEL);

  private IdentifierUtils() {
  }

  /**
   * Splits a possibly-quoted, dot-separated SQL identifier into its component parts, honoring
   * double-quote quoting so that a dot <em>inside</em> a quoted segment is treated as part of the
   * name rather than a path separator. For example {@code "VENICE"."my.table"} yields
   * {@code [VENICE, my.table]}, not {@code [VENICE, my, table]}.
   *
   * <p>Hoptimator accepts bare identifier strings on several paths (the {@code !graph}/
   * {@code !resolve}/{@code !describe} CLI commands, graph resolution, custom-resource lookups).
   * Those paths must not naively {@code split("\\.")}, or they corrupt names that legitimately
   * contain a dot.
   *
   * <p>The work splits cleanly by quoting:
   * <ul>
   *   <li><b>Unquoted input</b> ({@code no '"'}) is split directly on {@code .}. This is exact: an
   *       unquoted segment cannot itself contain a dot, so there is nothing for the SQL parser to
   *       disambiguate. It also covers names that are <em>not</em> valid standalone SQL identifiers
   *       but that Hoptimator accepts on its bare-string CLI commands — e.g. the unquoted
   *       hyphenated {@code LOGICAL.testevent-graph} (which the SQL grammar would read as the
   *       subtraction {@code testevent - graph}).</li>
   *   <li><b>Quoted input</b> is the <em>only</em> reason this helper exists: it runs the SQL
   *       parser so a dot inside a quoted segment ({@code "my.table"}) stays intact. If the quoted
   *       input does not parse to a single identifier — e.g. a malformed or mixed form like
   *       {@code "KAFKA".my-topic}, where an unquoted hyphenated segment is illegal — it throws
   *       rather than silently mis-splitting.</li>
   * </ul>
   *
   * @throws IllegalArgumentException if {@code identifier} is quoted but is not a well-formed
   *     dotted identifier.
   */
  public static List<String> parseIdentifier(String identifier) {
    if (identifier == null) {
      return new ArrayList<>();
    }
    // Unquoted: a plain dot-split is exact (no segment can contain a dot). This is also the path
    // for the unquoted hyphenated names Hoptimator's CLI commands accept but the SQL grammar does
    // not, so it must NOT go through the parser.
    if (identifier.indexOf('"') < 0) {
      return new ArrayList<>(Arrays.asList(identifier.split("\\.")));
    }
    // Quoted: use the parser so a dot inside a quoted segment is preserved.
    try {
      SqlNode node = SqlParser.create(identifier, PARSER_CONFIG).parseExpression();
      if (node instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) node;
        if (!id.isStar() && !id.names.isEmpty()) {
          return new ArrayList<>(id.names);
        }
      }
      // Parsed, but not a plain identifier (e.g. "KAFKA".my-topic reads as a subtraction).
      throw new IllegalArgumentException("Not a well-formed quoted table identifier: " + identifier);
    } catch (SqlParseException e) {
      throw new IllegalArgumentException("Not a well-formed quoted table identifier: " + identifier, e);
    }
  }
}

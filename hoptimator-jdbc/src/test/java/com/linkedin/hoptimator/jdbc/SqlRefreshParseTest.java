package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlRefreshObject;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class SqlRefreshParseTest {

  private static SqlRefreshObject parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql,
        SqlParser.config().withParserFactory(HoptimatorDdlExecutor.PARSER_FACTORY));
    SqlNode node = parser.parseStmt();
    assertThat(node).isInstanceOf(SqlRefreshObject.class);
    return (SqlRefreshObject) node;
  }

  private static String boundValue(SqlNode node) {
    return ((SqlLiteral) node).getValueAs(String.class);
  }

  @Test
  void plainRefreshHasNoWindow() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH \"foo\"");
    assertThat(refresh.from).isNull();
    assertThat(refresh.to).isNull();
    assertThat(refresh.name.names).containsExactly("foo");
  }

  @Test
  void compoundName() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH \"ADS\".\"MEMBERS\"");
    assertThat(refresh.name.names).containsExactly("ADS", "MEMBERS");
  }

  @Test
  void absoluteWindow() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH \"ADS\".\"MEMBERS\" FROM '2026-05-01' TO '2026-05-08'");
    assertThat(boundValue(refresh.from)).isEqualTo("2026-05-01");
    assertThat(boundValue(refresh.to)).isEqualTo("2026-05-08");
  }

  @Test
  void relativeWindow() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH \"foo\" FROM 7 DAYS AGO TO NOW");
    assertThat(boundValue(refresh.from)).isEqualTo("-7d");
    assertThat(boundValue(refresh.to)).isEqualTo("now");
  }

  @Test
  void rejectsKindKeyword() {
    // REFRESH targets a plain (physical) table — no MATERIALIZED VIEW / TABLE keyword.
    assertThatThrownBy(() -> parse("REFRESH TABLE \"foo\"")).isInstanceOf(SqlParseException.class);
    assertThatThrownBy(() -> parse("REFRESH MATERIALIZED VIEW \"foo\"")).isInstanceOf(SqlParseException.class);
  }

  @Test
  void rejectsWithClause() {
    assertThatThrownBy(() -> parse("REFRESH \"foo\" WITH ('k' 'v')")).isInstanceOf(SqlParseException.class);
  }
}

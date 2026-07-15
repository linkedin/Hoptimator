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
  void plainRefreshHasNoTypeOrWindow() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH \"foo\"");
    assertThat(refresh.objectType).isNull();
    assertThat(refresh.from).isNull();
    assertThat(refresh.to).isNull();
    assertThat(refresh.name.names).containsExactly("foo");
  }

  @Test
  void materializedViewKeyword() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH MATERIALIZED VIEW \"foo\"");
    assertThat(refresh.objectType).isEqualTo(SqlRefreshObject.ObjectType.MATERIALIZED_VIEW);
    assertThat(refresh.name.names).containsExactly("foo");
  }

  @Test
  void tableKeyword() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH TABLE \"foo\"");
    assertThat(refresh.objectType).isEqualTo(SqlRefreshObject.ObjectType.TABLE);
  }

  @Test
  void compoundName() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH TABLE \"SCHEMA\".\"foo\"");
    assertThat(refresh.objectType).isEqualTo(SqlRefreshObject.ObjectType.TABLE);
    assertThat(refresh.name.names).containsExactly("SCHEMA", "foo");
  }

  @Test
  void absoluteWindow() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH \"foo\" FROM '2026-05-01' TO '2026-05-08'");
    assertThat(boundValue(refresh.from)).isEqualTo("2026-05-01");
    assertThat(boundValue(refresh.to)).isEqualTo("2026-05-08");
  }

  @Test
  void relativeWindowWithType() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH TABLE \"foo\" FROM 7 DAYS AGO TO NOW");
    assertThat(refresh.objectType).isEqualTo(SqlRefreshObject.ObjectType.TABLE);
    assertThat(boundValue(refresh.from)).isEqualTo("-7d");
    assertThat(boundValue(refresh.to)).isEqualTo("now");
  }

  @Test
  void materializedViewWithWindow() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH MATERIALIZED VIEW \"foo\" FROM 2 HOURS AGO TO NOW");
    assertThat(refresh.objectType).isEqualTo(SqlRefreshObject.ObjectType.MATERIALIZED_VIEW);
    assertThat(boundValue(refresh.from)).isEqualTo("-2h");
    assertThat(boundValue(refresh.to)).isEqualTo("now");
  }

  @Test
  void rejectsWithClause() {
    // REFRESH is a pure imperative action — it takes no WITH options.
    assertThatThrownBy(() -> parse("REFRESH \"foo\" WITH ('k' 'v')"))
        .isInstanceOf(SqlParseException.class);
  }

  @Test
  void unparseRoundTripsMaterializedView() throws SqlParseException {
    SqlRefreshObject refresh = parse("REFRESH MATERIALIZED VIEW \"foo\" FROM '2026-05-01' TO '2026-05-08'");
    String sql = refresh.toString().replace("\n", " ").replaceAll(" +", " ");
    assertThat(sql).contains("REFRESH").contains("MATERIALIZED").contains("VIEW")
        .contains("FROM").contains("TO");
  }
}

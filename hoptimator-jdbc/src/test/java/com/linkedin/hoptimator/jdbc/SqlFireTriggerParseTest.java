package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlFireTrigger;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class SqlFireTriggerParseTest {

  private static SqlFireTrigger parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql,
        SqlParser.config().withParserFactory(HoptimatorDdlExecutor.PARSER_FACTORY));
    SqlNode node = parser.parseStmt();
    assertThat(node).isInstanceOf(SqlFireTrigger.class);
    return (SqlFireTrigger) node;
  }

  private static String boundValue(SqlNode node) {
    return ((SqlLiteral) node).getValueAs(String.class);
  }

  @Test
  void plainFireHasNoWindow() throws SqlParseException {
    SqlFireTrigger fire = parse("FIRE TRIGGER \"t\"");
    assertThat(fire.from).isNull();
    assertThat(fire.to).isNull();
  }

  @Test
  void absoluteWindow() throws SqlParseException {
    SqlFireTrigger fire = parse("FIRE TRIGGER \"t\" FROM '2026-05-01' TO '2026-05-08'");
    assertThat(boundValue(fire.from)).isEqualTo("2026-05-01");
    assertThat(boundValue(fire.to)).isEqualTo("2026-05-08");
  }

  @Test
  void relativeWindowEncodesOffsets() throws SqlParseException {
    SqlFireTrigger fire = parse("FIRE TRIGGER \"t\" FROM 7 DAYS AGO TO 1 DAY AGO");
    assertThat(boundValue(fire.from)).isEqualTo("-7d");
    assertThat(boundValue(fire.to)).isEqualTo("-1d");
  }

  @Test
  void nowBound() throws SqlParseException {
    SqlFireTrigger fire = parse("FIRE TRIGGER \"t\" FROM 2 HOURS AGO TO NOW");
    assertThat(boundValue(fire.from)).isEqualTo("-2h");
    assertThat(boundValue(fire.to)).isEqualTo("now");
  }

  @Test
  void rejectsWithClause() {
    // FIRE no longer accepts WITH options — it is a pure imperative action.
    assertThatThrownBy(() -> parse("FIRE TRIGGER \"t\" WITH ('k' 'v')"))
        .isInstanceOf(SqlParseException.class);
  }

  @Test
  void allRelativeUnits() throws SqlParseException {
    assertThat(boundValue(parse("FIRE TRIGGER \"t\" FROM 30 SECONDS AGO TO NOW").from)).isEqualTo("-30s");
    assertThat(boundValue(parse("FIRE TRIGGER \"t\" FROM 5 MINUTES AGO TO NOW").from)).isEqualTo("-5m");
    assertThat(boundValue(parse("FIRE TRIGGER \"t\" FROM 1 HOUR AGO TO NOW").from)).isEqualTo("-1h");
    assertThat(boundValue(parse("FIRE TRIGGER \"t\" FROM 2 DAYS AGO TO NOW").from)).isEqualTo("-2d");
  }

  // --- resolveFireBound (executor) ---

  @Test
  void resolveNow() {
    OffsetDateTime before = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
    OffsetDateTime resolved = OffsetDateTime.parse(HoptimatorDdlExecutor.resolveFireBound("now", null));
    assertThat(resolved).isBetween(before.minusSeconds(2), before.plusSeconds(5));
  }

  @Test
  void resolveRelativeIsBeforeNow() {
    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
    OffsetDateTime resolved = OffsetDateTime.parse(HoptimatorDdlExecutor.resolveFireBound("-7d", null));
    assertThat(resolved).isBefore(now.minusDays(6)).isAfter(now.minusDays(8));
  }

  @Test
  void resolveAbsoluteInstant() {
    assertThat(HoptimatorDdlExecutor.resolveFireBound("2026-05-01T00:00:00Z", null))
        .isEqualTo("2026-05-01T00:00Z");
  }

  @Test
  void resolveAbsoluteDate() {
    assertThat(HoptimatorDdlExecutor.resolveFireBound("2026-05-01", null))
        .isEqualTo("2026-05-01T00:00Z");
  }

  @Test
  void resolveRejectsGarbage() {
    SqlNode node = SqlLiteral.createCharString("someday", org.apache.calcite.sql.parser.SqlParserPos.ZERO);
    assertThatThrownBy(() -> HoptimatorDdlExecutor.resolveFireBound("someday", node))
        .isInstanceOf(HoptimatorDdlExecutor.DdlException.class)
        .hasMessageContaining("Invalid FIRE TRIGGER time bound");
  }
}

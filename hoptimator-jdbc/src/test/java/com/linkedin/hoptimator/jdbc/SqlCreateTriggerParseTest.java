package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTrigger;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class SqlCreateTriggerParseTest {

  private static SqlCreateTrigger parse(String sql) throws SqlParseException {
    SqlParser parser = SqlParser.create(sql,
        SqlParser.config().withParserFactory(HoptimatorDdlExecutor.PARSER_FACTORY));
    SqlNode node = parser.parseStmt();
    assertThat(node).isInstanceOf(SqlCreateTrigger.class);
    return (SqlCreateTrigger) node;
  }

  private static final String BASE = "CREATE TRIGGER \"t\" ON \"S\".\"TBL\" AS 'job-yaml'";

  @Test
  void parsesBareTrigger() throws SqlParseException {
    SqlCreateTrigger trigger = parse(BASE);
    assertThat(trigger.name.toString()).isEqualTo("t");
    assertThat(trigger.cron).isNull();
  }

  @Test
  void parsesTriggerWithSchedule() throws SqlParseException {
    SqlCreateTrigger trigger = parse(BASE + " SCHEDULED '@hourly'");
    assertThat(trigger.cron).isNotNull();
    assertThat(trigger.cron.toString()).contains("@hourly");
  }

  @Test
  void unparseRoundTripsSchedule() throws SqlParseException {
    SqlCreateTrigger trigger = parse(BASE + " SCHEDULED '@hourly'");
    assertThat(trigger.toString().toUpperCase()).contains("SCHEDULED");
  }

  @Test
  void rejectsRemovedLookBackClause() {
    // LOOK BACK / LOOK AHEAD were removed; jobs express their own read window in SQL instead.
    assertThatThrownBy(() -> parse(BASE + " LOOK BACK 5 MINUTES"))
        .isInstanceOf(SqlParseException.class);
  }
}

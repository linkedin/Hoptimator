package com.linkedin.hoptimator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class JobTest {

  @Test
  void testNameAndAccessors() {
    Sink sink = new Sink("db", List.of("s", "t"), Collections.emptyMap());
    Source src = new Source("db", List.of("s", "src"), Collections.emptyMap());
    Set<Source> sources = Set.of(src);
    Map<String, ThrowingFunction<SqlDialect, String>> evals = Map.of(
        "sql", d -> "INSERT INTO t SELECT * FROM src",
        "query", d -> "SELECT * FROM src",
        "fieldMap", d -> "{}"
    );

    Job job = new Job("myJob", sources, sink, evals);

    assertEquals("myJob", job.name());
    assertEquals(sources, job.sources());
    assertEquals(sink, job.sink());
  }

  @Test
  void testSqlReturnsFunction() throws Exception {
    Sink sink = new Sink("db", List.of("t"), Collections.emptyMap());
    Map<String, ThrowingFunction<SqlDialect, String>> evals = Map.of(
        "sql", d -> "my-sql",
        "query", d -> "my-query",
        "fieldMap", d -> "my-fields"
    );
    Job job = new Job("j", Collections.emptySet(), sink, evals);

    assertEquals("my-sql", job.sql().apply(SqlDialect.ANSI));
    assertEquals("my-query", job.query().apply(SqlDialect.FLINK));
    assertEquals("my-fields", job.fieldMap().apply(SqlDialect.ANSI));
  }

  @Test
  void testEvalThrowsForUnknownKey() {
    Sink sink = new Sink("db", List.of("t"), Collections.emptyMap());
    Map<String, ThrowingFunction<SqlDialect, String>> evals = Map.of(
        "sql", d -> "s"
    );
    Job job = new Job("j", Collections.emptySet(), sink, evals);

    assertThrows(IllegalArgumentException.class, () -> job.query());
  }

  @Test
  void testToString() {
    Sink sink = new Sink("db", List.of("schema", "table"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    assertEquals("Job[schema.table]", job.toString());
  }
}

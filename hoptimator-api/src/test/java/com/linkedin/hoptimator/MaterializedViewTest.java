package com.linkedin.hoptimator;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class MaterializedViewTest {

  @Test
  void testAccessors() {
    Sink sink = new Sink("db", List.of("sink"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    Pipeline pipeline = new Pipeline(Collections.emptyList(), sink, job);
    ThrowingFunction<SqlDialect, String> pSql = d -> "INSERT INTO sink SELECT * FROM src";

    MaterializedView mv = new MaterializedView("myDb", List.of("s", "v"), "SELECT 1", pSql, pipeline);

    assertEquals("myDb", mv.database());
    assertEquals(pipeline, mv.pipeline());
    assertNotNull(mv.pipelineSql());
    assertEquals("SELECT 1", mv.viewSql());
    assertEquals("v", mv.table());
    assertEquals("s", mv.schema());
  }

  @Test
  void testToString() {
    MaterializedView mv = new MaterializedView("db", List.of("a", "b"), "SELECT 1", d -> "", null);
    assertEquals("MaterializedView[a.b]", mv.toString());
  }
}

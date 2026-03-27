package com.linkedin.hoptimator;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class SinkTest {

  @Test
  void testToString() {
    Sink sink = new Sink("db", List.of("schema", "table"), Collections.emptyMap());
    assertEquals("Sink[schema.table]", sink.toString());
  }

  @Test
  void testInheritsSourceBehavior() {
    Sink sink = new Sink("myDb", List.of("cat", "sch", "tbl"), Collections.emptyMap());
    assertEquals("tbl", sink.table());
    assertEquals("sch", sink.schema());
    assertEquals("cat", sink.catalog());
    assertEquals("myDb", sink.database());
  }
}

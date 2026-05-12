package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


class TriggerTest {

  @Test
  void testAccessors() {
    UserJob userJob = new UserJob("ns", "jobName");
    Map<String, String> options = Map.of("paused", "true");
    Source source = new Source("db", List.of("schema", "table"), Collections.emptyMap());
    Trigger trigger = new Trigger("myTrigger", userJob, "0 * * * *", options, source);

    assertEquals("myTrigger", trigger.name());
    assertEquals(userJob, trigger.job());
    assertEquals("0 * * * *", trigger.cronSchedule());
    assertEquals(options, trigger.options());
    assertNotNull(trigger.source());
    assertEquals(List.of("schema", "table"), trigger.source().path());
    assertEquals("table", trigger.source().table());
    assertEquals("schema", trigger.source().schema());
    assertNull(trigger.sink());
  }

  @Test
  void testNullSourceAccessor() {
    Trigger trigger = new Trigger("t", null, null, Map.of(), null);
    assertNull(trigger.source());
    assertNull(trigger.sink());
  }

  @Test
  void testToString() {
    Source source = new Source("db", List.of("a", "b"), Collections.emptyMap());
    Trigger trigger = new Trigger("myTrig", null, null, Map.of(), source);
    assertEquals("Trigger[myTrig, a.b]", trigger.toString());
  }

  @Test
  void testToStringWithoutSource() {
    Trigger trigger = new Trigger("myTrig", null, null, Map.of(), null);
    assertEquals("Trigger[myTrig, <unbound>]", trigger.toString());
  }
}

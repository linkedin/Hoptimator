package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class TriggerTest {

  @Test
  void testAccessors() {
    UserJob userJob = new UserJob("ns", "jobName");
    Map<String, String> options = Map.of("paused", "true");
    Trigger trigger = new Trigger("myTrigger", userJob, List.of("schema", "table"), "0 * * * *", options);

    assertEquals("myTrigger", trigger.name());
    assertEquals(userJob, trigger.job());
    assertEquals(List.of("schema", "table"), trigger.path());
    assertEquals("0 * * * *", trigger.cronSchedule());
    assertEquals(options, trigger.options());
    assertEquals("table", trigger.table());
    assertEquals("schema", trigger.schema());
  }

  @Test
  void testSchemaReturnsNullForSingleElementPath() {
    Trigger trigger = new Trigger("t", null, List.of("table"), null, Map.of());
    assertNull(trigger.schema());
  }

  @Test
  void testToString() {
    Trigger trigger = new Trigger("myTrig", null, List.of("a", "b"), null, Map.of());
    assertEquals("Trigger[myTrig, a.b]", trigger.toString());
  }
}

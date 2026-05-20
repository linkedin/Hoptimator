package com.linkedin.hoptimator.graph.mermaid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * The bulk of the cron-grammar work happens inside {@code cron-utils}, so we don't re-test that
 * library here. These cases pin the wrapper contract: standard 5-field expressions get translated
 * to prose, and anything the parser rejects (Quartz 6-field, malformed input, null/empty) is
 * returned unchanged so the user always sees something useful in the trigger label.
 *
 * <p>Substring assertions intentionally — the exact phrasing belongs to cron-utils and shouldn't
 * lock us into a specific library version.
 */
class CronHumanizerTest {

  @Test
  void everyMinute() {
    String out = CronHumanizer.humanize("* * * * *");
    assertTrue(out.toLowerCase().contains("minute"),
        "expected a humanized phrase mentioning 'minute', got: " + out);
  }

  @Test
  void everyNMinutes() {
    String out = CronHumanizer.humanize("*/5 * * * *");
    assertTrue(out.contains("5") && out.toLowerCase().contains("minute"),
        "expected '5' and 'minute' in: " + out);
  }

  @Test
  void everyNHours() {
    String out = CronHumanizer.humanize("0 */6 * * *");
    assertTrue(out.contains("6") && out.toLowerCase().contains("hour"),
        "expected '6' and 'hour' in: " + out);
  }

  @Test
  void daily() {
    String out = CronHumanizer.humanize("0 0 * * *");
    // cron-utils renders this as "at 00:00" — no day-of-week qualifier when DOW is wildcard.
    assertTrue(out.contains("00:00"), "expected '00:00' in: " + out);
  }

  @Test
  void weekly() {
    String out = CronHumanizer.humanize("0 3 * * 1");
    assertTrue(out.contains("03:00") && out.toLowerCase().contains("monday"),
        "expected '03:00' and 'Monday' in: " + out);
  }

  @Test
  void unparseableExpressionsFallThroughUnchanged() {
    // Quartz 6-field — our CronDefinition is 5-field, so the parser rejects this.
    assertEquals("0 0 0 * * ?", CronHumanizer.humanize("0 0 0 * * ?"));
    // Garbage input — also rejected.
    assertEquals("not a cron", CronHumanizer.humanize("not a cron"));
  }

  @Test
  void nicknamesParse() {
    // CronDefinition opts in to @hourly / @daily / @weekly etc., so they should describe rather
    // than fall through. We don't pin exact phrasing.
    String hourly = CronHumanizer.humanize("@hourly");
    assertTrue(hourly.toLowerCase().contains("hour"), "expected 'hour' in: " + hourly);
  }

  @Test
  void nullAndEmptyPassThrough() {
    assertNull(CronHumanizer.humanize(null));
    assertEquals("", CronHumanizer.humanize(""));
  }
}

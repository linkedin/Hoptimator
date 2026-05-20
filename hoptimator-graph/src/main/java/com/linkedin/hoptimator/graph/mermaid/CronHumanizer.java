package com.linkedin.hoptimator.graph.mermaid;

import java.util.Locale;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Renders a cron expression as English prose for the Mermaid Trigger node label.
 *
 * <p>Delegates to {@code cron-utils}'s {@link CronDescriptor}, which already understands the
 * full grammar (ranges, lists, step values, nicknames). Anything {@link CronParser} rejects —
 * malformed input, foreign dialects we don't model — falls back to the raw expression so the
 * user still sees something useful instead of a wrong English phrase.
 *
 * <p>The cron grammar matches the one {@code TableTriggerReconciler} uses at runtime, so
 * what the visualizer accepts and what the operator accepts don't drift.
 */
final class CronHumanizer {

  private static final Logger log = LoggerFactory.getLogger(CronHumanizer.class);

  private static final CronDefinition CRON_DEFINITION = CronDefinitionBuilder.defineCron()
      .withMinutes().withValidRange(0, 59).withStrictRange().and()
      .withHours().withValidRange(0, 23).withStrictRange().and()
      .withDayOfMonth().withValidRange(1, 31).withStrictRange().and()
      .withMonth().withValidRange(1, 12).withStrictRange().and()
      .withDayOfWeek().withValidRange(0, 7).withMondayDoWValue(1).withIntMapping(7, 0).withStrictRange().and()
      .withSupportedNicknameHourly()
      .withSupportedNicknameDaily()
      .withSupportedNicknameWeekly()
      .withSupportedNicknameMonthly()
      .withSupportedNicknameYearly()
      .withSupportedNicknameAnnually()
      .withSupportedNicknameMidnight()
      .instance();

  private static final CronParser PARSER = new CronParser(CRON_DEFINITION);
  private static final CronDescriptor DESCRIPTOR = CronDescriptor.instance(Locale.US);

  private CronHumanizer() {
  }

  /** Render a cron expression as English prose, or return the input unchanged when parsing fails. */
  static String humanize(String cron) {
    if (cron == null || cron.isEmpty()) {
      return cron;
    }
    try {
      return DESCRIPTOR.describe(PARSER.parse(cron.trim()));
    } catch (IllegalArgumentException e) {
      // CronParser throws IllegalArgumentException for unrecognized expressions (Quartz 6-field
      // forms, vendor-specific syntax, etc.). Fall back to the raw form rather than swallow the
      // user's intent behind a wrong phrase.
      log.warn("Unable to parse cron expression={}", cron, e);
      return cron;
    }
  }
}

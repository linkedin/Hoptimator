package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;


/**
 * Unit tests for {@link DualLogger}.
 */
class DualLoggerTest {

  @Test
  void infoFansOutToHookWithClassPrefixAndFormatting() {
    List<String> logged = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("created {} in {}", "table", "schema");

    assertThat(logged).containsExactly("[DualLoggerTest] created table in schema");
  }

  @Test
  void infoInvokesEveryHook() {
    List<String> first = new ArrayList<>();
    List<String> second = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(first::add, second::add));

    logger.info("hello");

    assertThat(first).containsExactly("[DualLoggerTest] hello");
    assertThat(second).containsExactly("[DualLoggerTest] hello");
  }

  @Test
  void infoWithNoHooksDoesNotThrow() {
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of());

    // slf4j still receives the message; with no hooks there is nothing to assert but it must not throw.
    assertThatCode(() -> logger.info("no hooks {}", "here")).doesNotThrowAnyException();
  }
}

package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;


/**
 * Direct unit tests for {@link DualLogger}. It used to be exercised only via
 * {@link HoptimatorConnection}, where the logic lived; now that it is its own class it is tested on
 * its own — constructed from a class and a hook list, with no JDBC connection.
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

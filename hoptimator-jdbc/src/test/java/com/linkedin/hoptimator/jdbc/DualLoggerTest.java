package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;


/**
 * Unit tests for {@link DualLogger}. Every level shares the same fan-out behavior, so the shared
 * cases are parameterized across all of them.
 */
class DualLoggerTest {

  /** A level-specific logging call, e.g. {@link DualLogger#info}. */
  @FunctionalInterface
  private interface LogCall {
    void log(DualLogger logger, String format, Object... arguments);
  }

  private static Stream<Arguments> levels() {
    return Stream.of(
        Arguments.of("trace", (LogCall) DualLogger::trace),
        Arguments.of("debug", (LogCall) DualLogger::debug),
        Arguments.of("info", (LogCall) DualLogger::info),
        Arguments.of("warn", (LogCall) DualLogger::warn),
        Arguments.of("error", (LogCall) DualLogger::error));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("levels")
  void fansOutToHookWithClassPrefixAndFormatting(String level, LogCall call) {
    List<String> logged = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    call.log(logger, "created {} in {}", "table", "schema");

    assertThat(logged).containsExactly("[DualLoggerTest] created table in schema");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("levels")
  void invokesEveryHook(String level, LogCall call) {
    List<String> first = new ArrayList<>();
    List<String> second = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(first::add, second::add));

    call.log(logger, "hello");

    assertThat(first).containsExactly("[DualLoggerTest] hello");
    assertThat(second).containsExactly("[DualLoggerTest] hello");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("levels")
  void withNoHooksDoesNotThrow(String level, LogCall call) {
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of());

    // slf4j still receives the message; with no hooks there is nothing to assert but it must not throw.
    assertThatCode(() -> call.log(logger, "no hooks {}", "here")).doesNotThrowAnyException();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("levels")
  void withThrowableAppendsThrowableMessageToHooks(String level, LogCall call) {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    call.log(logger, "could not deploy {}", "table", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] could not deploy table: failed");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("levels")
  void withThrowableWithNullMessageFallsBackToToString(String level, LogCall call) {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    call.log(logger, "could not deploy {}", "table", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] could not deploy table: java.lang.RuntimeException");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("levels")
  void withThrowableAndNoHooksDoesNotThrow(String level, LogCall call) {
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of());

    // slf4j still receives the message; with no hooks there is nothing to assert but it must not throw.
    assertThatCode(() -> call.log(logger, "no hooks {}", "here", exception)).doesNotThrowAnyException();
  }

  @Test
  void withoutHooksListStillLogsToSlf4j() {
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of());

    assertThatCode(() -> logger.info("just slf4j {}", "here")).doesNotThrowAnyException();
  }

  // --- Argument-count x throwable matrix. fanOut is shared across levels, so info() exercises it. ---

  @Test
  void tooManyArgumentsWithoutThrowableIgnoresExtras() {
    List<String> logged = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("one {}", "x", "y");

    assertThat(logged).containsExactly("[DualLoggerTest] one x");
  }

  @Test
  void notEnoughArgumentsWithoutThrowableLeavesPlaceholderLiteral() {
    List<String> logged = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("two {} {}", "x");

    assertThat(logged).containsExactly("[DualLoggerTest] two x {}");
  }

  @Test
  void exactArgumentsWithoutThrowable() {
    List<String> logged = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("two {} {}", "x", "y");

    assertThat(logged).containsExactly("[DualLoggerTest] two x y");
  }

  @Test
  void noArgumentsLeavesPlaceholderLiteral() {
    List<String> logged = new ArrayList<>();
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("one {}");

    assertThat(logged).containsExactly("[DualLoggerTest] one {}");
  }

  @Test
  void throwableWithNotEnoughArgumentsLeavesPlaceholderAndAppendsThrowable() {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    // Two placeholders, one non-throwable arg: the trailing throwable is never used to fill a
    // placeholder, so the second placeholder stays literal and the throwable is appended.
    logger.info("two {} {}", "x", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] two x {}: failed");
  }

  @Test
  void throwableWithTooManyArgumentsIgnoresExtrasAndAppendsThrowable() {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("one {}", "x", "y", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] one x: failed");
  }

  @Test
  void throwableWithExactArgumentsAppendsThrowable() {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("one {}", "x", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] one x: failed");
  }

  @Test
  void throwableAsOnlyArgumentWithNoPlaceholderAppendsThrowable() {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    logger.info("deploy failed", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] deploy failed: failed");
  }

  @Test
  void throwableAsOnlyArgumentWithPlaceholderIsNotUsedToFillPlaceholder() {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    // The lone throwable is treated as the exception, not as the {} substitution.
    logger.info("deploy {}", exception);

    assertThat(logged).containsExactly("[DualLoggerTest] deploy {}: failed");
  }

  @Test
  void onlyNonTrailingThrowableIsTreatedAsRegularArgument() {
    List<String> logged = new ArrayList<>();
    RuntimeException exception = new RuntimeException("failed");
    DualLogger logger = new DualLogger(DualLoggerTest.class, List.of(logged::add));

    // A throwable that is NOT the last argument is a normal parameter, not the exception.
    logger.info("deploy {} {}", exception, "x");

    assertThat(logged).containsExactly("[DualLoggerTest] deploy java.lang.RuntimeException: failed x");
  }
}

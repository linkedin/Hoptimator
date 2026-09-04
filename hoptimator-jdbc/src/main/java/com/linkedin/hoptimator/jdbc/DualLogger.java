package com.linkedin.hoptimator.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.util.List;
import java.util.function.Consumer;


/**
 * A logger that fans out each message to both an SLF4J logger and a list of log hooks. Used by both
 * the SQL DDL path (via {@link HoptimatorConnection#getLogger}) and the connection-free direct path
 * (constructed directly from log hooks); it depends only on a class — for the SLF4J logger and the
 * message prefix — and the hook list, never on a JDBC connection.
 */
final class DualLogger {
  private final String className;
  private final Logger slf4jLogger;
  private final List<Consumer<String>> hooks;

  DualLogger(Class<?> clazz, List<Consumer<String>> hooks) {
    this.className = clazz.getSimpleName();
    this.slf4jLogger = LoggerFactory.getLogger(clazz);
    this.hooks = hooks;
  }

  /**
   * Log a message with slf4j format at the TRACE level. If the last argument is a {@link Throwable},
   * SLF4J logs it with a stack trace and its message is appended to the text sent to the hooks.
   */
  public void trace(String format, Object... arguments) {
    slf4jLogger.trace(format, arguments);
    fanOut(format, arguments);
  }

  /**
   * Log a message with slf4j format at the DEBUG level. If the last argument is a {@link Throwable},
   * SLF4J logs it with a stack trace and its message is appended to the text sent to the hooks.
   */
  public void debug(String format, Object... arguments) {
    slf4jLogger.debug(format, arguments);
    fanOut(format, arguments);
  }

  /**
   * Log a message with slf4j format at the INFO level. If the last argument is a {@link Throwable},
   * SLF4J logs it with a stack trace and its message is appended to the text sent to the hooks.
   */
  public void info(String format, Object... arguments) {
    slf4jLogger.info(format, arguments);
    fanOut(format, arguments);
  }

  /**
   * Log a message with slf4j format at the WARN level. If the last argument is a {@link Throwable},
   * SLF4J logs it with a stack trace and its message is appended to the text sent to the hooks.
   */
  public void warn(String format, Object... arguments) {
    slf4jLogger.warn(format, arguments);
    fanOut(format, arguments);
  }

  /**
   * Log a message with slf4j format at the ERROR level. If the last argument is a {@link Throwable},
   * SLF4J logs it with a stack trace and its message is appended to the text sent to the hooks.
   */
  public void error(String format, Object... arguments) {
    slf4jLogger.error(format, arguments);
    fanOut(format, arguments);
  }

  /**
   * Formats the message, prefixes it with the class name, appends the throwable detail (if any), and
   * fans the result out to every hook. The slf4j logging happens in the level-specific methods.
   */
  private void fanOut(String format, Object... arguments) {
    FormattingTuple tuple = MessageFormatter.arrayFormat(format, arguments);
    String msgWithClassName = String.format("[%s] %s", className, tuple.getMessage());
    Throwable throwable = tuple.getThrowable();
    if (throwable != null) {
      String detail = throwable.getMessage() != null ? throwable.getMessage() : throwable.toString();
      msgWithClassName = String.format("%s: %s", msgWithClassName, detail);
    }
    String finalMsg = msgWithClassName;
    hooks.forEach(hook -> hook.accept(finalMsg));
  }
}

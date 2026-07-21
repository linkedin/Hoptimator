package com.linkedin.hoptimator.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
   * Log a message with slf4j format at the INFO level.
   */
  public void info(String format, Object... arguments) {
    slf4jLogger.info(format, arguments);
    String msg = MessageFormatter.arrayFormat(format, arguments).getMessage();
    String msgWithClassName = String.format("[%s] %s", className, msg);
    hooks.forEach(hook -> hook.accept(msgWithClassName));
  }
}

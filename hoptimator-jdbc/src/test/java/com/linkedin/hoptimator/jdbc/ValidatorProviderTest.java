package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


/**
 * SPI-registered ValidatorProvider used in unit tests. Registered via
 * META-INF/services/com.linkedin.hoptimator.ValidatorProvider so that
 * ServiceLoader finds it on the test classpath.
 *
 * <p>Behavior is controlled by static flags so individual tests can configure
 * it without needing to create anonymous subclasses in every test.
 */
public class ValidatorProviderTest implements ValidatorProvider {

  /** When true, every call to validate() records an error into issues. */
  private static final AtomicBoolean SHOULD_ERROR = new AtomicBoolean(false);

  /** Tracks the most-recent object passed to validators(). */
  private static final AtomicReference<Object> LAST_SEEN = new AtomicReference<>();

  static void reset() {
    SHOULD_ERROR.set(false);
    LAST_SEEN.set(null);
  }

  static void enableErrors() {
    SHOULD_ERROR.set(true);
  }

  static Object lastSeen() {
    return LAST_SEEN.get();
  }

  @Override
  public <T> Collection<Validator> validators(T obj, Connection connection) {
    LAST_SEEN.set(obj);
    if (SHOULD_ERROR.get()) {
      return Collections.singletonList((issues, conn) -> issues.error("ValidatorProviderTest injected error"));
    }
    return Collections.emptyList();
  }
}

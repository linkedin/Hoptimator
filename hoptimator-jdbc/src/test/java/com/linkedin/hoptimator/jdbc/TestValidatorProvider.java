package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * SPI-registered ValidatorProvider used in unit tests. Registered via
 * META-INF/services/com.linkedin.hoptimator.ValidatorProvider so that
 * ServiceLoader finds it on the test classpath.
 *
 * <p>Behaviour is controlled by static flags so individual tests can configure
 * it without needing to create anonymous subclasses in every test.
 */
@SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
    justification = "Intentional: static state used to control test behavior across SPI-loaded instances")
public class TestValidatorProvider implements ValidatorProvider {

  /** When true, every call to validate() records an error into issues. */
  private static final AtomicBoolean SHOULD_ERROR = new AtomicBoolean(false);

  /** Tracks the most-recent object passed to validators(). */
  private static volatile Object lastSeen = null;

  static void reset() {
    SHOULD_ERROR.set(false);
    lastSeen = null;
  }

  static void enableErrors() {
    SHOULD_ERROR.set(true);
  }

  static Object lastSeen() {
    return lastSeen;
  }

  @Override
  public <T> Collection<Validator> validators(T obj) {
    lastSeen = obj;
    if (SHOULD_ERROR.get()) {
      return Collections.singletonList(issues -> issues.error("TestValidatorProvider injected error"));
    }
    return Collections.emptyList();
  }
}

package com.linkedin.hoptimator;

import java.util.Objects;


/**
 * A type-tagged wrapper signalling that {@code target} is about to be deleted. Pre-delete
 * validators (e.g. dependency guards that block DROP TABLE when a pipeline still references the
 * resource) should key off this wrapper rather than the raw target type — so an unrelated
 * future caller of {@code ValidationService.validateOrThrow(source, connection)} doesn't
 * accidentally trigger delete-intent checks.
 *
 * <p>Pattern:
 * <pre>{@code
 *   ValidationService.validateOrThrow(new PendingDelete<>(source), connection);
 * }</pre>
 */
public final class PendingDelete<T> {

  private final T target;

  public PendingDelete(T target) {
    this.target = Objects.requireNonNull(target, "target");
  }

  public T target() {
    return target;
  }

  @Override
  public String toString() {
    return "PendingDelete[" + target + "]";
  }
}

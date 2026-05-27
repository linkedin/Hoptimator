package com.linkedin.hoptimator;

import java.util.Objects;
import javax.annotation.Nullable;


/**
 * A type-tagged wrapper signaling that {@code target} is about to be deleted. Pre-delete
 * validators (e.g. dependency guards that block DROP TABLE when a pipeline still references
 * the resource) key off this wrapper rather than the raw target type — so an unrelated future
 * caller of {@code ValidationService.validateOrThrow(source, connection)} doesn't accidentally
 * trigger delete-intent checks.
 *
 * <p>An optional {@code (selfOwnerKind, selfOwnerName)} lets the caller declare an "umbrella"
 * K8s resource whose owned objects should be excluded from the dependent set — e.g. a
 * LogicalTable CRD, so its child Pipeline CRDs (which reference tier sources by SQL) don't
 * self-block the drop.
 */
public final class PendingDelete<T> {

  private final T target;
  private final String selfOwnerKind;
  private final String selfOwnerName;

  public PendingDelete(T target) {
    this(target, null, null);
  }

  public PendingDelete(T target, @Nullable String selfOwnerKind, @Nullable String selfOwnerName) {
    this.target = Objects.requireNonNull(target, "target");
    this.selfOwnerKind = selfOwnerKind;
    this.selfOwnerName = selfOwnerName;
  }

  public T target() {
    return target;
  }

  /** Kind of the K8s resource whose owned objects should be excluded from the dependent set. */
  public @Nullable String selfOwnerKind() {
    return selfOwnerKind;
  }

  /** Name of the K8s resource whose owned objects should be excluded from the dependent set. */
  public @Nullable String selfOwnerName() {
    return selfOwnerName;
  }

  @Override
  public String toString() {
    String self = (selfOwnerKind != null && selfOwnerName != null)
        ? ", self=" + selfOwnerKind + "/" + selfOwnerName : "";
    return "PendingDelete[" + target + self + "]";
  }
}

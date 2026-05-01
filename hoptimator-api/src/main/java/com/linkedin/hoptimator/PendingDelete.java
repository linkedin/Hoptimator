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
 * <p>An optional {@code selfOwnerUid} lets the caller declare an "umbrella" K8s resource UID
 * whose owned objects should be excluded from the dependent set — e.g. a LogicalTable CRD's UID,
 * so its child Pipeline CRDs (which reference tier sources by SQL) don't self-block the drop.
 */
public final class PendingDelete<T> {

  private final T target;
  private final String selfOwnerUid;

  public PendingDelete(T target) {
    this(target, null);
  }

  public PendingDelete(T target, @Nullable String selfOwnerUid) {
    this.target = Objects.requireNonNull(target, "target");
    this.selfOwnerUid = selfOwnerUid;
  }

  public T target() {
    return target;
  }

  /** UID of the K8s resource whose owned objects should be excluded from the dependent set. */
  public @Nullable String selfOwnerUid() {
    return selfOwnerUid;
  }

  @Override
  public String toString() {
    return "PendingDelete[" + target + (selfOwnerUid != null ? ", self=" + selfOwnerUid : "") + "]";
  }
}

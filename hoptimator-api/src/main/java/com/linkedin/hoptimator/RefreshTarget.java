package com.linkedin.hoptimator;

import java.util.Collections;
import java.util.List;


/**
 * The result of resolving a {@code REFRESH} target: what kind of object it is, and the names of the
 * triggers immediately upstream of it (the triggers a REFRESH should fire).
 *
 * <p>Resolving a target is a backend concern (e.g. Kubernetes label/owner lookup) exposed through
 * {@link RefreshProvider}, so the DDL layer never needs to know how triggers are wired to their
 * downstream materialized view or logical table.
 */
public final class RefreshTarget {

  /** The kind of object being refreshed. Mirrors the optional {@code MATERIALIZED VIEW}/{@code TABLE}
   *  keyword in the DDL, so the executor can reject a mismatched assertion. */
  public enum Kind {
    MATERIALIZED_VIEW,
    TABLE
  }

  private final Kind kind;
  private final List<String> triggerNames;

  public RefreshTarget(Kind kind, List<String> triggerNames) {
    this.kind = kind;
    this.triggerNames = triggerNames == null ? Collections.emptyList()
        : Collections.unmodifiableList(triggerNames);
  }

  /** The resolved object kind. */
  public Kind kind() {
    return kind;
  }

  /** Names of the triggers immediately upstream of the object. May be empty when the object exists
   *  but has no upstream triggers — the executor treats that as an error, since a REFRESH that
   *  silently fires nothing is a footgun. */
  public List<String> triggerNames() {
    return triggerNames;
  }

  @Override
  public String toString() {
    return "RefreshTarget[" + kind + ", triggers=" + triggerNames + "]";
  }
}

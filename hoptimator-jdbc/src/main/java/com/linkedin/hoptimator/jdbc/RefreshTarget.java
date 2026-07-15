package com.linkedin.hoptimator.jdbc;

import java.util.Collections;
import java.util.List;


/**
 * The result of resolving a {@code REFRESH} target: what kind of object it is, and the names of the
 * triggers immediately upstream of it (the triggers a REFRESH should fire).
 *
 * <p>Resolved by {@link RefreshService} from the pipeline dependency graph, so the DDL layer never
 * needs to know how the backend wires triggers to their downstream materialized view or logical
 * table.
 */
final class RefreshTarget {

  /** The kind of object being refreshed. Mirrors the optional {@code MATERIALIZED VIEW}/{@code TABLE}
   *  keyword in the DDL, so the executor can reject a mismatched assertion. */
  enum Kind {
    MATERIALIZED_VIEW,
    TABLE
  }

  private final Kind kind;
  private final List<String> triggerNames;

  RefreshTarget(Kind kind, List<String> triggerNames) {
    this.kind = kind;
    this.triggerNames = triggerNames == null ? Collections.emptyList()
        : Collections.unmodifiableList(triggerNames);
  }

  /** The resolved object kind. */
  Kind kind() {
    return kind;
  }

  /** Names of the triggers immediately upstream of the object. May be empty when the object exists
   *  but has no upstream triggers — the executor treats that as an error, since a REFRESH that
   *  silently fires nothing is a footgun. */
  List<String> triggerNames() {
    return triggerNames;
  }

  @Override
  public String toString() {
    return "RefreshTarget[" + kind + ", triggers=" + triggerNames + "]";
  }
}

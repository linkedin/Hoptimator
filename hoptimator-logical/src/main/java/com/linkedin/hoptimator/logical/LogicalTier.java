package com.linkedin.hoptimator.logical;

import java.util.Arrays;
import java.util.Optional;


/**
 * The set of tier names recognized by the logical table driver.
 *
 * <p>Only JDBC URL parameters whose keys match one of these tier names are treated
 * as tier declarations; all other parameters are ignored.
 */
public enum LogicalTier {

  NEARLINE("nearline"),
  OFFLINE("offline"),
  ONLINE("online");

  private final String tierName;

  LogicalTier(String tierName) {
    this.tierName = tierName;
  }

  /** The lowercase tier name used in JDBC URL params and K8s labels. */
  public String tierName() {
    return tierName;
  }

  /** Returns the tier matching the given name, or empty if not a recognised tier. */
  public static Optional<LogicalTier> of(String name) {
    return Arrays.stream(values())
        .filter(t -> t.tierName.equalsIgnoreCase(name))
        .findFirst();
  }

  /** Returns true if the given name is a recognised tier name. */
  public static boolean isTier(String name) {
    return of(name).isPresent();
  }
}

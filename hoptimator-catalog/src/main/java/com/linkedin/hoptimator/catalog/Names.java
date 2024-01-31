package com.linkedin.hoptimator.catalog;

import java.util.Locale;

public final class Names {

  private Names() {
  }

  /** Attempt to format s as a K8s object name, or part of one. */
  public static String canonicalize(String s) {
    return s.toLowerCase(Locale.ROOT)
      .replaceAll("[^a-z0-9\\-]+", "-")
      .replaceAll("^[^a-z0-9]*", "")
      .replaceAll("[^a-z0-9]*$", "")
      .replaceAll("\\-+", "-");
  }

}

package com.linkedin.hoptimator.catalog;

import java.util.Collection;

/** In Hoptimator, Tables can have baggage in the form of Resources. */
public interface ResourceProvider {

  /** Resources for the given table */
  Collection<Resource> resources(String tableName);
}

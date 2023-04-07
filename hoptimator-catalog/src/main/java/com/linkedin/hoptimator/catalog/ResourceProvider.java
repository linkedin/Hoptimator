package com.linkedin.hoptimator.catalog;

import java.util.Collection;

/** In Hoptimator, Tables can have baggage in the form of Resources. */
public interface ResourceProvider {

  Collection<Resource> resources();
}

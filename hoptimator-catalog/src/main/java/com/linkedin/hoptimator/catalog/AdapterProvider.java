package com.linkedin.hoptimator.catalog;

import java.util.Collection;

/** Service Provider Interface for extending Hoptimator via Adapters. */
public interface AdapterProvider {
  Collection<Adapter> adapters();
}

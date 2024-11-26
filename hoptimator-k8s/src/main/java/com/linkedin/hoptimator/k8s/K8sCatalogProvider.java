package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.CatalogProvider;

import java.util.Collection;
import java.util.Collections;

/** Provides the `k8s` catalog and related metadata tables. */
public class K8sCatalogProvider implements CatalogProvider {

  @Override
  public Collection<Catalog> catalogs() {
    return Collections.singletonList(new K8sCatalog());
  }
}

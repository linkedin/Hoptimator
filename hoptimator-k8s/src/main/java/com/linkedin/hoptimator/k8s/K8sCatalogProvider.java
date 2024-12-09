package com.linkedin.hoptimator.k8s;

import java.util.Collection;
import java.util.Collections;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.CatalogProvider;


/** Provides the `k8s` catalog and related metadata tables. */
public class K8sCatalogProvider implements CatalogProvider {

  @Override
  public Collection<Catalog> catalogs() {
    return Collections.singletonList(new K8sCatalog());
  }
}

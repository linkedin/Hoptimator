package com.linkedin.hoptimator.jdbc;

import java.util.Collection;
import java.util.Collections;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.CatalogProvider;
import com.linkedin.hoptimator.jdbc.schema.UtilityCatalog;


public class BuiltinCatalogProvider implements CatalogProvider {

  @Override
  public Collection<Catalog> catalogs() {
    return Collections.singletonList(new UtilityCatalog());
  }
}

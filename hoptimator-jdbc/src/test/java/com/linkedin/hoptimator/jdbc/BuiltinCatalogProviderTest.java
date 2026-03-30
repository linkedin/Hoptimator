package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Catalog;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class BuiltinCatalogProviderTest {

  @Test
  void testCatalogsReturnsSingleCatalog() {
    BuiltinCatalogProvider provider = new BuiltinCatalogProvider();

    Collection<Catalog> catalogs = provider.catalogs();

    assertEquals(1, catalogs.size());
  }

  @Test
  void testCatalogsContainsUtilityCatalog() {
    BuiltinCatalogProvider provider = new BuiltinCatalogProvider();

    Collection<Catalog> catalogs = provider.catalogs();
    Catalog catalog = catalogs.iterator().next();

    assertNotNull(catalog);
    assertEquals("util", catalog.name());
  }

  @Test
  void testCatalogsContainsUtilityCatalogWithDescription() {
    BuiltinCatalogProvider provider = new BuiltinCatalogProvider();

    Catalog catalog = provider.catalogs().iterator().next();

    assertNotNull(catalog.description());
    assertTrue(catalog.description().length() > 0);
  }
}

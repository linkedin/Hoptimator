package com.linkedin.hoptimator.k8s;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


class K8sCatalogProviderTest {

  @Test
  void catalogsReturnsSingleCatalog() {
    K8sCatalogProvider provider = new K8sCatalogProvider();
    Collection<Catalog> catalogs = provider.catalogs();
    assertEquals(1, catalogs.size());
  }

  @Test
  void catalogHasCorrectName() {
    K8sCatalogProvider provider = new K8sCatalogProvider();
    Catalog catalog = provider.catalogs().iterator().next();
    assertEquals("k8s", catalog.name());
  }

  @Test
  void catalogHasDescription() {
    K8sCatalogProvider provider = new K8sCatalogProvider();
    Catalog catalog = provider.catalogs().iterator().next();
    assertFalse(catalog.description().isEmpty());
  }
}

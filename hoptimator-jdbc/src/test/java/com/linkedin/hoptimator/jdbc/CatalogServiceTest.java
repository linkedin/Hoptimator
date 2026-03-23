package com.linkedin.hoptimator.jdbc;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.CatalogProvider;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class CatalogServiceTest {

  @Test
  void testProvidersReturnsNonNullCollection() {
    Collection<CatalogProvider> providers = CatalogService.providers();

    assertNotNull(providers);
  }

  @Test
  void testCatalogsReturnsNonNullCollection() {
    Collection<Catalog> catalogs = CatalogService.catalogs();

    assertNotNull(catalogs);
  }

  @Test
  void testCatalogsContainsUtilCatalog() {
    Collection<Catalog> catalogs = CatalogService.catalogs();

    // BuiltinCatalogProvider is registered via SPI and provides "util"
    boolean hasUtil = catalogs.stream().anyMatch(c -> "util".equals(c.name()));
    assertTrue(hasUtil);
  }

  @Test
  void testCatalogFindsUtilByName() {
    Catalog catalog = CatalogService.catalog("util");

    assertNotNull(catalog);
    assertTrue("util".equalsIgnoreCase(catalog.name()));
  }

  @Test
  void testCatalogThrowsForUnknownName() {
    assertThrows(IllegalArgumentException.class,
        () -> CatalogService.catalog("nonexistent-catalog-xyz"));
  }

  @Test
  void testApiFieldIsNotNull() {
    assertNotNull(CatalogService.API);
  }

  @Test
  void testApiListReturnsCatalogs() throws Exception {
    Collection<Catalog> catalogs = CatalogService.API.list();

    assertNotNull(catalogs);
  }
}

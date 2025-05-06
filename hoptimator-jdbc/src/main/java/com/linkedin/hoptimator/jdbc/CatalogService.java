package com.linkedin.hoptimator.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.CatalogProvider;
import com.linkedin.hoptimator.util.Api;


public final class CatalogService {

  private CatalogService() {
  }

  // This bit of magic means we can trivially implement a RemoteTable:
  public static final Api<Catalog> API = CatalogService::catalogs;

  public static Collection<CatalogProvider> providers() {
    ServiceLoader<CatalogProvider> loader = ServiceLoader.load(CatalogProvider.class);
    List<CatalogProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  public static Collection<Catalog> catalogs() {
    return providers().stream().flatMap(x -> x.catalogs().stream()).collect(Collectors.toList());
  }

  /** Load a specific catalog */
  public static Catalog catalog(String name) {
    return providers().stream()
        .flatMap(x -> x.catalogs().stream())
        .filter(x -> name.equalsIgnoreCase(x.name()))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Catalog " + name + " not found."));
  }
}

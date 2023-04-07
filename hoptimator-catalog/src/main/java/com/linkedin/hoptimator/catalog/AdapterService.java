package com.linkedin.hoptimator.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Dynamically loads Adapters via the AdapterProvider SPI. */
public final class AdapterService {
  private static final AdapterService INSTANCE = new AdapterService(providers());
  private final Map<String, Adapter> adapters;

  private AdapterService(Collection<AdapterProvider> providers) {
    this.adapters = providers.stream().flatMap(x -> x.adapters().stream())
      .collect(Collectors.toMap(x -> x.database(), x -> x));
  }

  private static Collection<AdapterProvider> providers() {
    ServiceLoader<AdapterProvider> loader = ServiceLoader.load(AdapterProvider.class);
    List<AdapterProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  public static Adapter adapter(String database) {
    return INSTANCE.adapters.computeIfAbsent(database, x -> new Adapter() {
      @Override
      public String database() {
        return database;
      }

      @Override
      public Collection<String> list() {
        return Collections.emptyList();
      }

      @Override
      public AdapterTable table(String table) {
        throw new IllegalArgumentException("No table '" + table + "' because the '" + database + "' adapter is missing.");
      }
    });
  }

  public static Collection<Adapter> adapters() {
    return INSTANCE.adapters.values();
  }
}

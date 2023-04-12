package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptPlanner;

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
  private final List<RelOptRule> rules;

  private AdapterService(Collection<AdapterProvider> providers) {
    this.adapters = providers.stream().flatMap(x -> x.adapters().stream())
      .collect(Collectors.toMap(x -> x.database(), x -> x));
    this.rules = providers.stream().flatMap(x -> x.rules().stream())
      .collect(Collectors.toList());
  }

  private static Collection<AdapterProvider> providers() {
    ServiceLoader<AdapterProvider> loader = ServiceLoader.load(AdapterProvider.class);
    List<AdapterProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  /** Find a specific Adapter, or return an empty placeholder Adapter */
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

  /** All loaded Adapters */
  public static Collection<Adapter> adapters() {
    return Collections.unmodifiableCollection(INSTANCE.adapters.values());
  }

  /** All loaded additional planner rules */
  public static Collection<RelOptRule> rules() {
    return Collections.unmodifiableCollection(INSTANCE.rules);
  }

  /** Register all rules with the given planner */
  public static void registerRules(RelOptPlanner planner) {
    INSTANCE.rules.forEach(x -> planner.addRule(x));
  }
}

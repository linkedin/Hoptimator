package com.linkedin.hoptimator.util;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import com.google.common.base.Splitter;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import com.linkedin.hoptimator.util.planner.PipelineRules;


public final class DeploymentService {

  static final String HINT_OPTION = "hints";
  public static final String PIPELINE_OPTION = "pipeline";

  private DeploymentService() {
  }

  public static void create(Collection<Deployer> deployers)
      throws SQLException {
    for (Deployer deployer : deployers) {
      deployer.create();
    }
  }

  public static void delete(Collection<Deployer> deployers)
      throws SQLException {
    for (Deployer deployer : deployers) {
      deployer.delete();
    }
  }

  public static void update(Collection<Deployer> deployers)
      throws SQLException {
    for (Deployer deployer : deployers) {
      deployer.update();
    }
  }

  // Since nothing about specify needs to be stateful, the deployers can be fetched on demand
  public static <T extends Deployable> List<String> specify(T obj, Connection connection)
      throws SQLException {
    List<String> specs = new ArrayList<>();
    for (Deployer deployer : deployers(obj, connection)) {
      specs.addAll(deployer.specify());
    }
    return specs;
  }

  public static void restore(Collection<Deployer> deployers) {
    for (Deployer deployer : deployers) {
      deployer.restore();
    }
  }

  public static Collection<DeployerProvider> providers() {
    ServiceLoader<DeployerProvider> loader = ServiceLoader.load(DeployerProvider.class);
    List<DeployerProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    providers.sort(Comparator.comparingInt(DeployerProvider::priority));
    return providers;
  }

  public static <T extends Deployable> Collection<Deployer> deployers(T obj, Connection connection) {
    Collection<DeployerProvider> providers = providers();

    // Filter out base classes when subclasses exist
    Set<DeployerProvider> filteredProviders = new HashSet<>();
    for (DeployerProvider provider : providers) {
      boolean hasSubclass = false;
      for (DeployerProvider other : providers) {
        if (other != provider && provider.getClass().isAssignableFrom(other.getClass())) {
          // 'other' is a subclass of 'provider', so skip 'provider'
          hasSubclass = true;
          break;
        }
      }
      if (!hasSubclass) {
        filteredProviders.add(provider);
      }
    }

    // Now collect deployers from filtered providers
    return filteredProviders.stream()
        .flatMap(x -> x.deployers(obj, connection).stream())
        .collect(Collectors.toList());
  }

  /** Plans a deployable Pipeline which implements the query. */
  public static PipelineRel.Implementor plan(RelRoot root, List<RelOptMaterialization> materializations,
      Properties connectionProperties) throws SQLException {
    RelTraitSet traitSet = root.rel.getTraitSet().simplify().replace(PipelineRel.CONVENTION);

    // We need to run the plan without field trimming enabled. The intention of field trimming is to optimize
    // away unused fields. Calcite is able to make micro optimizations to the plan but at the cost of making
    // no guarantees about what that fields will be named in the resulting RelNode. For the ScriptImplementor,
    // field names are important because they are used to generate the final SQL against the sink table. This will
    // almost always require some top-level Project in the plan, but with trimming enabled, identity projects
    // (just field renames) are optimized out of the plan.
    // See discussion in https://issues.apache.org/jira/browse/CALCITE-1297
    Program program = Programs.standard(DefaultRelMetadataProvider.INSTANCE, false);

    RelOptPlanner planner = root.rel.getCluster().getPlanner();
    PipelineRules.rules().forEach(planner::addRule);
    PipelineRel plan = (PipelineRel) program.run(planner, root.rel, traitSet, materializations,
        Collections.emptyList());
    PipelineRel.Implementor implementor = new PipelineRel.Implementor(root.fields, parseHints(connectionProperties));
    implementor.visit(plan);
    return implementor;
  }

  // User provided hints will be passed through the "hints" field as KEY=VALUE pairs separated by commas.
  // Values containing commas or other special characters should be URL-encoded.
  // Both keys and values are automatically URL-decoded to support special characters.
  // We can also configure additional properties to pass through as hints to the deployer.
  public static Map<String, String> parseHints(Properties connectionProperties) {
    Map<String, String> hints = new LinkedHashMap<>();
    if (connectionProperties.containsKey(HINT_OPTION)) {
      String property = connectionProperties.getProperty(HINT_OPTION);
      if (property != null && !property.isEmpty()) {
        Map<String, String> rawHints = Splitter.on(',').withKeyValueSeparator('=').split(property);
        // URL-decode both keys and values to support special characters
        for (Map.Entry<String, String> entry : rawHints.entrySet()) {
          String key = urlDecode(entry.getKey());
          String value = urlDecode(entry.getValue());
          hints.put(key, value);
        }
      }
    }

    if (connectionProperties.containsKey(PIPELINE_OPTION)) {
      String property = connectionProperties.getProperty(PIPELINE_OPTION);
      if (property != null && !property.isEmpty()) {
        hints.put(PIPELINE_OPTION, property);
      }
    }

    return hints;
  }

  /**
   * URL-decodes a string, returning the original string if decoding fails.
   * This allows both encoded and non-encoded values to work.
   *
   * @param value the string to decode
   * @return the decoded string, or the original if decoding fails
   */
  private static String urlDecode(String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      // If decoding fails, return the original value (backward compatibility)
      return value;
    }
  }
}

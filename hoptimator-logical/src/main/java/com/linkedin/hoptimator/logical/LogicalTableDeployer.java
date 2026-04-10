package com.linkedin.hoptimator.logical;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sPipelineBundle;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.util.DeploymentService;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;


/**
 * Deploys a logical table by:
 * <ol>
 *   <li>Validating that at least two tiers are defined.</li>
 *   <li>Creating physical resources for each tier via the tier-specific deployer SPI.</li>
 *   <li>Creating a {@code LogicalTable} CRD via {@link K8sLogicalTableCrdDeployer}.</li>
 *   <li>Creating implicit inter-tier Pipeline CRDs owned by the LogicalTable CRD.</li>
 * </ol>
 */
public class LogicalTableDeployer implements Deployer, Validated {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployer.class);

  private final Source source;
  private final Properties tierProps;
  private final K8sContext context;
  private final K8sApi<V1alpha1Database, V1alpha1DatabaseList> databasesApi;

  private final List<Deployer> tierDeployers = new ArrayList<>();
  private final List<K8sPipelineBundle> pipelineDeployers = new ArrayList<>();
  private final List<Runnable> schemaRollbacks = new ArrayList<>();

  // Created lazily in deployAll(). Null until then, so restore() is a no-op for the CRD if
  // deployAll() never ran (e.g. validation failed before deployment started).
  private K8sLogicalTableCrdDeployer logicalTableCrdDeployer;

  // Cached state built during validate() and reused in create()/update().
  // If validate() was not called, deployAll() builds these on demand.
  private Map<String, V1alpha1Database> cachedTierDatabases;
  private Map<String, Source> cachedTierSources;

  LogicalTableDeployer(Source source, Properties tierProps, K8sContext context) {
    this(source, tierProps, context, new K8sApi<>(context, K8sApiEndpoints.DATABASES));
  }

  /** Package-private constructor for testing — accepts an injectable database API. */
  LogicalTableDeployer(Source source, Properties tierProps, K8sContext context,
      K8sApi<V1alpha1Database, V1alpha1DatabaseList> databasesApi) {
    this.source = source;
    this.tierProps = tierProps;
    this.context = context;
    this.databasesApi = databasesApi;
  }

  /**
   * Factory method for the LogicalTable CRD deployer — overridable in tests.
   * The deployer inherits snapshot-based restore from {@link com.linkedin.hoptimator.k8s.K8sDeployer}:
   * if the CRD didn't exist before this deployment it is deleted on restore(); if it existed it
   * is reverted to its prior state.
   */
  K8sLogicalTableCrdDeployer createLogicalTableCrdDeployer(
      String crdName, String databaseLabel, Map<String, String> tierMap) {
    return new K8sLogicalTableCrdDeployer(crdName, databaseLabel, source.table(), tierMap, context);
  }

  /**
   * Validates the logical table configuration and any existing tier resources.
   * Builds and caches tier connections and deployers for reuse in create()/update().
   *
   * <p>Called by {@link com.linkedin.hoptimator.jdbc.ValidationService} before deployment.
   */
  @Override
  public void validate(Validator.Issues issues) {
    try {
      // Pre-register the row type in tier schemas so deployers (e.g. VeniceDeployer) can
      // call HoptimatorDriver.rowType() during their own validate() calls.
      ensureTierRowTypesRegistered();
      // These methods are self-caching; subsequent calls return the cached result.
      for (Map.Entry<String, Source> entry : buildTierSources().entrySet()) {
        Source tierSource = entry.getValue();
        Collection<Deployer> deployers = DeploymentService.deployers(tierSource, context.connection());
        for (Deployer deployer : deployers) {
          if (deployer instanceof Validated) {
            ((Validated) deployer).validate(issues);
          }
        }
      }
    } catch (SQLException e) {
      issues.error("Logical table '" + source.table() + "' validation failed: " + e.getMessage());
    }
  }

  /**
   * Registers a {@link com.linkedin.hoptimator.jdbc.TemporaryTable} in each tier schema
   * so deployers can call {@link com.linkedin.hoptimator.jdbc.HoptimatorDriver#rowType}
   * before the physical table exists. Idempotent — if already registered (non-empty
   * {@code schemaRollbacks}), returns immediately. Rollbacks are stored so that
   * {@link #restore()} can undo the registrations on failure.
   */
  private void ensureTierRowTypesRegistered() throws SQLException {
    if (!schemaRollbacks.isEmpty()) {
      return; // Already registered (e.g. validate() was called before create/update).
    }
    if (context == null) {
      return;
    }
    HoptimatorConnection conn = context.connection();
    if (conn == null) {
      return;
    }
    RelDataType rowType = HoptimatorDriver.rowType(source, conn);
    for (Source tierSource : buildTierSources().values()) {
      schemaRollbacks.add(HoptimatorDdlUtils.registerTemporaryTableInSchema(
          conn, tierSource.catalog(), tierSource.schema(),
          tierSource.table(), rowType, tierSource.database()));
    }
  }

  private static List<String> pathFromDatabase(V1alpha1Database database, String table) {
    V1alpha1DatabaseSpec databaseSpec = database.getSpec();
    List<String> path = new ArrayList<>();
    if (databaseSpec.getCatalog() != null) {
      path.add(databaseSpec.getCatalog());
    }
    if (databaseSpec.getSchema() != null) {
      path.add(databaseSpec.getSchema());
    }
    path.add(table);
    return path;
  }

  @Override
  public void create() throws SQLException {
    deployAll(false);
  }

  @Override
  public void update() throws SQLException {
    deployAll(true);
  }

  /**
   * Core create/update logic. Tier resources are always deployed with update() semantics
   * (create-or-update). The LogicalTable CRD is managed by {@link K8sLogicalTableCrdDeployer}
   * which snapshots the prior state before acting, enabling correct restore() behavior.
   */
  private void deployAll(boolean update) throws SQLException {
    Map<String, String> tierMap = buildTierMap();
    Map<String, V1alpha1Database> tierDatabases = resolveTiers();
    Map<String, Source> tierSources = buildTierSources();
    try {
      deployTierResources(tierSources);

      String crdName = K8sUtils.canonicalizeName(source.path());
      logicalTableCrdDeployer = createLogicalTableCrdDeployer(crdName, source.database(), tierMap);
      V1OwnerReference ownerRef = update
          ? logicalTableCrdDeployer.updateAndReference()
          : logicalTableCrdDeployer.createAndReference();
      K8sContext ownerContext = context.withOwner(ownerRef);
      deployImplicitPipelines(tierMap, tierDatabases, tierSources, ownerContext, update);
    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw e;
      }
      throw new SQLException((update ? "Failed to update" : "Failed to create")
          + " logical table " + source.table(), e);
    }
  }

  @Override
  public void delete() throws SQLException {
    // TODO: Implement safe logical table deletion.
    // Deletion is blocked until we can verify no active pipelines depend on this table.
    // See: LogicalTableDeployer.delete()
    throw new SQLFeatureNotSupportedException(
        "Logical table deletion is not yet supported. "
        + "Cannot safely delete physical tier resources without verifying no active pipelines "
        + "depend on this table.");
  }

  @Override
  public void restore() {
    // Restore pipeline deployers first (they reference the LogicalTable CRD via ownerRef)
    for (int i = pipelineDeployers.size() - 1; i >= 0; i--) {
      pipelineDeployers.get(i).restore();
    }
    // Delegate CRD rollback to the deployer: if it didn't exist before, it's deleted;
    // if it existed, it's reverted to its prior state; if deployAll() never ran it's null.
    if (logicalTableCrdDeployer != null) {
      logicalTableCrdDeployer.restore();
    }
    // Restore tier deployers in reverse
    for (int i = tierDeployers.size() - 1; i >= 0; i--) {
      tierDeployers.get(i).restore();
    }
    // Restore tier schema snapshots (removes or restores temporary table registrations)
    for (Runnable rollback : schemaRollbacks) {
      rollback.run();
    }
    schemaRollbacks.clear(); // Reset so re-registration occurs on the next attempt.
  }

  /**
   * Returns the YAML specs that would be created for this logical table — tier source
   * resources (e.g. KafkaTopic) plus pipeline job elements (e.g. FlinkSessionJob).
   * Used by the {@code !specify} Quidem command to preview what a {@code CREATE TABLE}
   * would produce without actually deploying anything.
   */
  @Override
  public List<String> specify() throws SQLException {
    // Register tier temp tables so planPipeline() can resolve them via HoptimatorDriver.convert().
    // These are cleaned up by restore() called from the specifyCreateTable() caller.
    ensureTierRowTypesRegistered();
    Map<String, String> tierMap = buildTierMap();
    Map<String, Source> tierSources = buildTierSources();
    List<String> specs = new ArrayList<>();
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.ONLINE.tierName())) {
      String pipelineName = pipelineName(source.table(), LogicalTier.NEARLINE.tierName(), LogicalTier.ONLINE.tierName());
      Source fromSource = tierSources.get(LogicalTier.NEARLINE.tierName());
      Source toSource = tierSources.get(LogicalTier.ONLINE.tierName());
      specs.addAll(DeploymentService.specify(fromSource, context.connection()));
      specs.addAll(DeploymentService.specify(toSource, context.connection()));
      try {
        Pipeline pipeline = planPipeline(fromSource, toSource, pipelineName);
        specs.addAll(DeploymentService.specify(pipeline.job(), context.connection()));
      } catch (Exception e) {
        String message = String.format("Pipeline spec generation failed for %s on table %s", pipelineName, source.table());
        log.error(message, e);
        throw new SQLException(message, e);
      }
    }
    return specs;
  }

  // ─── Private helpers ────────────────────────────────────────────────────────

  /** Builds the Source for each tier, caching them for reuse across validate/create/update. */
  private Map<String, Source> buildTierSources() throws SQLException {
    if (cachedTierSources == null) {
      Map<String, String> tierMap = buildTierMap();
      Map<String, V1alpha1Database> tierDatabases = resolveTiers();
      Map<String, Source> result = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : tierMap.entrySet()) {
        String tierName = entry.getKey();
        // TODO: Figure out how to pass through tier specific options
        result.put(tierName, new Source(entry.getValue(),
            pathFromDatabase(tierDatabases.get(tierName), source.table()), source.options()));
      }
      cachedTierSources = result;
    }
    return cachedTierSources;
  }

  /** Builds the tier map from the connection properties. */
  Map<String, String> buildTierMap() {
    Map<String, String> tierMap = new LinkedHashMap<>();
    for (LogicalTier tier : LogicalTier.values()) {
      String tierName = tier.tierName();
      if (tierProps.containsKey(tierName)) {
        tierMap.put(tierName, tierProps.getProperty(tierName));
      }
    }
    return tierMap;
  }

  /**
   * Resolves each tier's Database CRD in a single pass.
   */
  private Map<String, V1alpha1Database> resolveTiers() throws SQLException {
    if (cachedTierDatabases == null) {
      Map<String, String> tierMap = buildTierMap();
      Map<String, V1alpha1Database> result = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : tierMap.entrySet()) {
        result.put(entry.getKey(), databasesApi.get(entry.getValue()));
      }
      cachedTierDatabases = result;
    }
    return cachedTierDatabases;
  }

  /**
   * Creates or updates physical resources for each tier using the deployer SPI.
   * Created deployers are tracked in {@link #tierDeployers} for rollback.
   */
  private void deployTierResources(Map<String, Source> tierSources) throws SQLException {
    // Ensure tier schemas have TemporaryTable entries so deployers can call
    // HoptimatorDriver.rowType(). This is idempotent — if validate() already ran
    // and registered, this is a no-op.
    ensureTierRowTypesRegistered();

    for (Map.Entry<String, Source> entry : tierSources.entrySet()) {
      String tierName = entry.getKey();
      Source tierSource = entry.getValue();
      log.info("Deploying tier {} (database CRD: {}) for table {}",
          tierName, tierSource.database(), source.table());
      Collection<Deployer> deployers = DeploymentService.deployers(tierSource, context.connection());
      for (Deployer deployer : deployers) {
        // Always use update() — it is create-or-update, making deployment idempotent
        // if tier resources already exist (e.g. from a previous partial run).
        deployer.update();
        tierDeployers.add(deployer);
      }
    }
  }

  /**
   * Creates or updates implicit inter-tier pipeline CRDs.
   * Each created bundle is tracked in {@link #pipelineDeployers}.
   */
  private void deployImplicitPipelines(Map<String, String> tierMap,
      Map<String, V1alpha1Database> tierDatabases, Map<String, Source> tierSources,
      K8sContext ownerContext, boolean update) throws SQLException {
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.ONLINE.tierName())) {
      deployPipelineBundle(LogicalTier.NEARLINE.tierName(), LogicalTier.ONLINE.tierName(), tierDatabases, tierSources,
          ownerContext, update);
    }
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.OFFLINE.tierName())) {
      log.debug("nearline → offline pending; skipping pipeline for {}", source.table());
    }
  }

  /**
   * Deploys a single implicit pipeline between two tiers using the full pipeline planner,
   * producing a proper {@link Job} with correct SQL, fieldMap, and connector configs.
   */
  private void deployPipelineBundle(String fromTier, String toTier, Map<String, V1alpha1Database> tierDatabases,
      Map<String, Source> tierSources, K8sContext ownerContext, boolean update) throws SQLException {
    String tableName = source.table();
    String pipelineName = pipelineName(tableName, fromTier, toTier);
    Source fromSource = tierSources.get(fromTier);
    Source toSource = tierSources.get(toTier);

    List<String> pipelineSpecs = new ArrayList<>();
    pipelineSpecs.addAll(DeploymentService.specify(fromSource, context.connection()));
    pipelineSpecs.addAll(DeploymentService.specify(toSource, context.connection()));

    String pipelineSql;
    try {
      Pipeline pipeline = planPipeline(fromSource, toSource, pipelineName);
      pipelineSql = pipeline.job().sql().apply(SqlDialect.ANSI);
      pipelineSpecs.addAll(DeploymentService.specify(pipeline.job(), context.connection()));
    } catch (SQLException e) {
      log.warn("Pipeline planner failed for {}->{} on table {}", fromTier, toTier, tableName, e);
      throw e;
    } catch (Exception e) {
      String message = String.format("Pipeline planner failed for %s->%s on table %s", fromTier, toTier, tableName);
      log.error(message, e);
      throw new SQLNonTransientException(message, e);
    }

    K8sPipelineBundle bundle = new K8sPipelineBundle(pipelineName, pipelineSpecs, pipelineSql, ownerContext);
    pipelineDeployers.add(bundle);
    if (update) {
      bundle.update();
    } else {
      bundle.create();
    }
  }

  /**
   * Plans the inter-tier pipeline, constructing the {@link PipelineRel.Implementor} directly
   * (bypassing the Calcite rule-based planner) for a trivial SELECT * → INSERT flow.
   * Shared by both {@link #deployPipelineBundle} and {@link #specify}.
   */
  private Pipeline planPipeline(Source fromSource, Source toSource, String pipelineName)
      throws Exception {
    HoptimatorConnection conn = context.connection();
    Properties props = conn.connectionProperties();
    props.setProperty(DeploymentService.PIPELINE_OPTION, pipelineName);
    RelRoot root = HoptimatorDriver.convert(conn, buildSelectSql(fromSource)).root;
    PipelineRel.Implementor plan = new PipelineRel.Implementor(
        root.fields, DeploymentService.parseHints(props));
    plan.addSource(fromSource.database(), fromSource.path(), root.rel.getRowType(), Collections.emptyMap());
    plan.setSink(toSource.database(), toSource.path(), root.rel.getRowType(), Collections.emptyMap());
    plan.setQuery(root.rel);
    return plan.pipeline(pipelineName, conn);
  }

  /** Returns the canonical pipeline name for an implicit inter-tier pipeline. */
  static String pipelineName(String tableName, String fromTier, String toTier) {
    return "logical-" + K8sUtils.canonicalizeName(tableName) + "-" + fromTier + "-to-" + toTier;
  }

  /** Builds a {@code SELECT * FROM ...} SQL for the given source, quoting each non-null
   *  path component individually to support 1-part (table), 2-part (schema.table), and
   *  3-part (catalog.schema.table) addressing. */
  static String buildSelectSql(Source source) {
    StringBuilder sb = new StringBuilder("SELECT * FROM ");
    if (source.catalog() != null) {
      sb.append("\"").append(source.catalog()).append("\".");
    }
    if (source.schema() != null) {
      sb.append("\"").append(source.schema()).append("\".");
    }
    sb.append("\"").append(source.table()).append("\"");
    return sb.toString();
  }
}

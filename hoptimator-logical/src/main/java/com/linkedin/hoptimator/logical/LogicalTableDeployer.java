package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.UserJob;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sPipelineBundle;
import com.linkedin.hoptimator.k8s.K8sTriggerDeployer;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.LogicalTableNames;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.util.DeploymentService;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.ValidationService;


/**
 * Deploys a logical table by:
 * <ol>
 *   <li>Validating that at least two tiers are defined.</li>
 *   <li>Creating physical resources for each tier via the tier-specific deployer SPI.</li>
 *   <li>Creating a {@code LogicalTable} CRD via {@link K8sLogicalTableDeployer}.</li>
 *   <li>Creating implicit inter-tier Pipeline CRDs owned by the LogicalTable CRD.</li>
 * </ol>
 *
 * TODO: this deployer is specific to k8s at this point in time, this should be refactored to be
 * deployment agnostic as possible, k8s specific pieces belong in the hoptimator-k8s module.
 */
public class LogicalTableDeployer implements Deployer, Validated {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployer.class);

  private final Source source;
  private final Properties tierProps;
  private final K8sContext context;
  private final K8sApi<V1alpha1Database, V1alpha1DatabaseList> databasesApi;

  private final List<Deployer> tierDeployers = new ArrayList<>();
  private final List<K8sPipelineBundle> pipelineDeployers = new ArrayList<>();
  private final List<Deployer> triggerDeployers = new ArrayList<>();
  private final List<Runnable> schemaRollbacks = new ArrayList<>();

  // Created lazily in deployAll(). Null until then, so restore() is a no-op for the CRD if
  // deployAll() never ran (e.g. validation failed before deployment started).
  private K8sLogicalTableDeployer logicalTableDeployer;

  // Cached state built during validate() and reused in create()/update().
  // If validate() was not called, deployAll() builds these on demand.
  private Map<String, V1alpha1Database> cachedTierDatabases;
  private Map<String, Source> cachedTierSources;

  LogicalTableDeployer(Source source, Properties tierProps, K8sContext context) {
    this(source, tierProps, context, new K8sApi<>(context, K8sApiEndpoints.DATABASES));
  }

  /** Package-private constructor for testing — accepts an injectable Database K8s API. */
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
  K8sLogicalTableDeployer createLogicalTableDeployer(
      String crdName, String databaseLabel, Map<String, String> tierMap) {
    return new K8sLogicalTableDeployer(crdName, databaseLabel, source.table(), tierMap, context);
  }

  /**
   * Validates the logical table configuration and any existing tier resources.
   * Builds and caches tier connections and deployers for reuse in create()/update().
   *
   * <p>Called by {@link com.linkedin.hoptimator.jdbc.ValidationService} before deployment.
   */
  @Override
  public void validate(Validator.Issues issues, Connection connection) {
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
            ((Validated) deployer).validate(issues, connection);
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
    path.add(databaseSpec.getSchema());
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
   * (create-or-update). The LogicalTable CRD is managed by {@link K8sLogicalTableDeployer}
   * which snapshots the prior state before acting, enabling correct restore() behavior.
   */
  private void deployAll(boolean update) throws SQLException {
    Map<String, String> tierMap = buildTierMap();
    Map<String, Source> tierSources = buildTierSources();
    try {
      deployTierResources(tierSources);

      String crdName = K8sUtils.canonicalizeName(source.path());
      logicalTableDeployer = createLogicalTableDeployer(crdName, source.database(), tierMap);
      V1OwnerReference ownerRef = update
          ? logicalTableDeployer.updateAndReference()
          : logicalTableDeployer.createAndReference();
      K8sContext ownerContext = context.withOwner(ownerRef);
      deployImplicitPipelines(tierMap, tierSources, ownerContext, update);
      deployImplicitTrigger(tierMap, tierSources, ownerContext);
    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw e;
      }
      throw new SQLException((update ? "Failed to update" : "Failed to create")
          + " logical table " + source.table(), e);
    }
  }

  /**
   * Deletes a logical table.
   *
   * <p>A logical DROP is structurally equivalent to running DROP TABLE on each tier plus
   * deleting the LogicalTable CRD. We mirror that shape exactly: each tier goes through the
   * same {@code validateOrThrow → DeploymentService.delete} pipeline a standalone DROP would.
   * The {@link PendingDelete}'s {@code (selfOwnerKind, selfOwnerName)} pair identifies the
   * LogicalTable CRD so the implicit inter-tier pipelines (owned by the CRD, cascade-deleted
   * with it) are excluded from the dependent set — only <em>external</em> pipelines block.
   *
   * <ol>
   *   <li>Per-tier dep check via the validator framework. Any active external pipeline blocks.
   *   <li>Delete the {@code LogicalTable} CRD. K8s owner-ref cascade removes its implicit
   *       inter-tier Pipelines and their Flink/YAML children. <b>Must succeed</b>.
   *   <li>Per-tier physical cleanup (Kafka topic, Venice store, ...). <b>Best effort</b> — a
   *       stranded tier resource is recoverable; aborting mid-DROP isn't.
   *   <li>Per-tier schema cleanup (deregister the {@code TemporaryTable} in tier schemas).
   * </ol>
   */
  @Override
  public void delete() throws SQLException {
    Map<String, Source> tierSources = buildTierSources();
    HoptimatorConnection conn = context.connection();
    String selfName = K8sUtils.canonicalizeName(source.path());

    // 1. Per-tier pre-flight dep check.
    for (Source tierSource : tierSources.values()) {
      ValidationService.validateOrThrow(
          new PendingDelete<>(tierSource, "LogicalTable", selfName), conn);
    }

    // 2. Delete the LogicalTable CRD (cascades owned pipelines/triggers).
    createLogicalTableDeployer(selfName, source.database(), buildTierMap()).delete();

    // 3. Per-tier physical cleanup. Best-effort: only deregister a tier's schema entry when its
    //    physical delete succeeded; failed tiers keep their entries so the user can retry.
    for (Source tierSource : tierSources.values()) {
      boolean tierSucceeded = true;
      for (Deployer deployer : DeploymentService.deployers(tierSource, conn)) {
        try {
          deployer.delete();
        } catch (Exception e) {
          tierSucceeded = false;
          log.warn("Tier cleanup failed for {} (continuing): {}",
              tierSource.pathString(), e.getMessage(), e);
        }
      }
      if (tierSucceeded) {
        HoptimatorDdlUtils.removeTableFromSchema(conn,
            tierSource.catalog(), tierSource.schema(), tierSource.table());
      }
    }
  }

  @Override
  public void restore() {
    // Restore trigger deployers first (they reference the LogicalTable CRD via ownerRef)
    for (int i = triggerDeployers.size() - 1; i >= 0; i--) {
      triggerDeployers.get(i).restore();
    }
    // Restore pipeline deployers next (they also reference the LogicalTable CRD via ownerRef)
    for (int i = pipelineDeployers.size() - 1; i >= 0; i--) {
      pipelineDeployers.get(i).restore();
    }
    // Delegate CRD rollback to the deployer: if it didn't exist before, it's deleted;
    // if it existed, it's reverted to its prior state; if deployAll() never ran it's null.
    if (logicalTableDeployer != null) {
      logicalTableDeployer.restore();
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
   * Returns the YAML specs that would be created for this logical table — mirrors
   * {@link #deployAll} but collects specs instead of deploying resources.
   *
   * <p>Step 1: tier resource specs via {@link DeploymentService#specify} on each
   * {@code tierSource} (same Sources used by {@link #deployTierResources}), so the
   * correct deployers are found via the tier's database name.
   *
   * <p>Step 2: pipeline job specs — plans each implicit inter-tier pipeline and
   * collects only the job artifacts. Sources and sink are already covered in step 1,
   * so only {@code pipeline.job()} specs are added to avoid duplicates.
   */
  @Override
  public List<String> specify() throws SQLException {
    // Register tier temp tables so specifyFromSql can resolve them via HoptimatorDriver.convert().
    // These are cleaned up by DeploymentService.restore(deployers) in processCreateTable after
    // SPECIFY completes — see HoptimatorDdlUtils.processCreateTable.
    ensureTierRowTypesRegistered();
    Map<String, String> tierMap = buildTierMap();
    Map<String, Source> tierSources = buildTierSources();
    List<String> specs = new ArrayList<>();

    // Step 1: tier resource specs (mirrors deployTierResources)
    for (Source tierSource : tierSources.values()) {
      specs.addAll(DeploymentService.specify(tierSource, context.connection()));
    }

    // Step 2: pipeline job specs (mirrors deployImplicitPipelines)
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.ONLINE.tierName())) {
      specs.addAll(specifyPipelineJob(
          tierSources.get(LogicalTier.NEARLINE.tierName()),
          tierSources.get(LogicalTier.ONLINE.tierName()),
          pipelineName(source.table(), LogicalTier.NEARLINE.tierName(), LogicalTier.ONLINE.tierName())));
    }
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.OFFLINE.tierName())) {
      specs.addAll(specifyPipelineJob(
          tierSources.get(LogicalTier.NEARLINE.tierName()),
          tierSources.get(LogicalTier.OFFLINE.tierName()),
          pipelineName(source.table(), LogicalTier.NEARLINE.tierName(), LogicalTier.OFFLINE.tierName())));
    }

    return specs;
  }

  /**
   * Plans the inter-tier pipeline by directly constructing a {@link PipelineRel.Implementor}
   * for the trivial {@code SELECT * FROM fromSource → INSERT INTO toSource} flow.
   *
   * <p>The rule-based planner ({@link DeploymentService#plan}) cannot handle
   * {@code LogicalTableModify} (INSERT INTO) → {@code PIPELINE} conversion because no such
   * rule exists in the pipeline rule set. We therefore bypass the planner and manually wire
   * source, sink, and query — the same approach as the original implementation.
   *
   * <p>Shared by {@link #deployPipelineBundle} and {@link #specifyPipelineJob}.
   */
  private Pipeline planPipeline(Source fromSource, Source toSource, String pipelineName) throws Exception {
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

  /** Plans the pipeline and returns only the job artifact specs (sources/sink already in step 1). */
  private List<String> specifyPipelineJob(Source fromSource, Source toSource, String pipelineName)
      throws SQLException {
    try {
      Pipeline pipeline = planPipeline(fromSource, toSource, pipelineName);
      return DeploymentService.specify(pipeline.job(), context.connection());
    } catch (Exception e) {
      String message = String.format("Pipeline spec generation failed for %s on table %s",
          pipelineName, source.table());
      log.error(message, e);
      throw new SQLException(message, e);
    }
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
  private void deployImplicitPipelines(Map<String, String> tierMap, Map<String, Source> tierSources,
      K8sContext ownerContext, boolean update) throws SQLException {
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.ONLINE.tierName())) {
      deployPipelineBundle(LogicalTier.NEARLINE.tierName(), LogicalTier.ONLINE.tierName(), tierSources,
          ownerContext, update);
    }
    if (tierMap.containsKey(LogicalTier.NEARLINE.tierName()) && tierMap.containsKey(LogicalTier.OFFLINE.tierName())) {
      deployPipelineBundle(LogicalTier.NEARLINE.tierName(), LogicalTier.OFFLINE.tierName(), tierSources,
          ownerContext, update);
    }
  }

  /**
   * Deploys a single implicit pipeline between two tiers using the full pipeline planner,
   * producing a proper {@link Job} with correct SQL, fieldMap, and connector configs.
   */
  void deployPipelineBundle(String fromTier, String toTier, Map<String, Source> tierSources,
      K8sContext ownerContext, boolean update) throws SQLException {
    String tableName = source.table();
    String pipelineName = pipelineName(tableName, fromTier, toTier);
    Source fromSource = tierSources.get(fromTier);
    Source toSource = tierSources.get(toTier);

    Pipeline pipeline;
    try {
      pipeline = planPipeline(fromSource, toSource, pipelineName);
    } catch (SQLException e) {
      log.warn("Pipeline planner failed for {}->{} on table {}", fromTier, toTier, tableName, e);
      throw e;
    } catch (Exception e) {
      String message = String.format("Pipeline planner failed for %s->%s on table %s", fromTier, toTier, tableName);
      log.error(message, e);
      throw new SQLNonTransientException(message, e);
    }

    HoptimatorConnection conn = context.connection();
    String pipelineSql = pipeline.job().sql().apply(SqlDialect.ANSI);
    List<String> pipelineSpecs = new ArrayList<>();
    for (Source src : pipeline.sources()) {
      pipelineSpecs.addAll(DeploymentService.specify(src, conn));
    }
    pipelineSpecs.addAll(DeploymentService.specify(pipeline.sink(), conn));
    pipelineSpecs.addAll(DeploymentService.specify(pipeline.job(), conn));

    K8sPipelineBundle bundle = new K8sPipelineBundle(pipelineName, pipelineSpecs, pipelineSql,
        pipeline.sources(), pipeline.sink(), ownerContext);
    pipelineDeployers.add(bundle);
    if (update) {
      bundle.update();
    } else {
      bundle.create();
    }
  }

  /** Returns the canonical pipeline name for an implicit inter-tier pipeline. */
  static String pipelineName(String tableName, String fromTier, String toTier) {
    return LogicalTableNames.pipelineName(tableName, fromTier, toTier);
  }

  /**
   * Creates or updates a {@link V1alpha1TableTrigger} for the offline tier, owned by the
   * LogicalTable CRD.
   */
  private void deployImplicitTrigger(Map<String, String> tierMap, Map<String, Source> tierSources, K8sContext ownerContext)
      throws SQLException {
    if (!tierMap.containsKey(LogicalTier.OFFLINE.tierName())) {
      return;
    }
    Source offlineSource = tierSources.get(LogicalTier.OFFLINE.tierName());
    String offlineDatabase = offlineSource.database();

    Source onlineSource = tierSources.get(LogicalTier.ONLINE.tierName());

    V1alpha1JobTemplate jobTemplate = findMatchingJobTemplate(ownerContext, offlineDatabase);
    if (jobTemplate == null) {
      return;
    }

    String triggerName = LogicalTableNames.triggerName(source.table());

    V1alpha1TableTrigger existing = null;
    try {
      existing = createTableTriggerApi(ownerContext).getIfExists(ownerContext.namespace(), triggerName);
    } catch (Exception e) {
      log.warn("Existence check for implicit trigger {} failed; assuming first deployment: {}",
          triggerName, e.toString());
    }

    Map<String, String> triggerOptions = new HashMap<>(source.options());
    if (existing == null) {
      triggerOptions.put(Trigger.PAUSED_OPTION, "true");
    }
    // When the offline tier feeds an online tier (reverse-ETL), the trigger has a downstream
    // sink — record it directly on the Trigger so the dep-guard can pick it up.
    Sink sink = onlineSource == null ? null
        : new Sink(onlineSource.database(), onlineSource.path(), onlineSource.options());
    UserJob userJob = new UserJob(ownerContext.namespace(), jobTemplate.getMetadata().getName());
    Trigger trigger = new Trigger(triggerName, userJob, null, triggerOptions, offlineSource, sink);

    K8sTriggerDeployer triggerDeployer = createTriggerDeployer(trigger, ownerContext);
    triggerDeployers.add(triggerDeployer);
    if (existing == null) {
      triggerDeployer.create();
    } else {
      triggerDeployer.update();
    }
    log.info("Deployed offline-tier trigger {} for logical table {} using JobTemplate {}",
        triggerName, source.table(), jobTemplate.getMetadata().getName());
  }

  private V1alpha1JobTemplate findMatchingJobTemplate(K8sContext ownerContext, String offlineDatabase) {
    Collection<V1alpha1JobTemplate> jobTemplates;
    try {
      jobTemplates = createJobTemplateApi(ownerContext).list();
    } catch (Exception e) {
      log.error("Error listing JobTemplates in namespace {}; skipping implicit trigger creation for offline database {}: {}",
          ownerContext.namespace(), offlineDatabase, e.toString());
      return null;
    }

    return jobTemplates.stream()
        .filter(template -> template.getSpec() != null
            && template.getSpec().getDatabases() != null
            && template.getSpec().getDatabases().contains(offlineDatabase))
        .findFirst().orElse(null);
  }

  K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> createJobTemplateApi(K8sContext ctx) {
    return new K8sApi<>(ctx, K8sApiEndpoints.JOB_TEMPLATES);
  }

  K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> createTableTriggerApi(K8sContext ctx) {
    return new K8sApi<>(ctx, K8sApiEndpoints.TABLE_TRIGGERS);
  }

  K8sTriggerDeployer createTriggerDeployer(Trigger trigger, K8sContext ctx) {
    return new K8sTriggerDeployer(trigger, ctx);
  }

  /**
   * Builds a {@code SELECT * FROM source} SQL statement used by {@link #planPipeline}.
   * Quoting each non-null path component to support 1-, 2-, and 3-part addressing.
   */
  static String buildSelectSql(Source source) {
    return "SELECT * FROM " + qualifiedRef(source);
  }

  /**
   * Builds an {@code INSERT INTO toSource SELECT * FROM fromSource} SQL statement,
   * quoting each non-null path component to support 1-, 2-, and 3-part addressing.
   */
  static String buildInsertSql(Source toSource, Source fromSource) {
    return "INSERT INTO " + qualifiedRef(toSource) + " SELECT * FROM " + qualifiedRef(fromSource);
  }

  private static String qualifiedRef(Source source) {
    StringBuilder sb = new StringBuilder();
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

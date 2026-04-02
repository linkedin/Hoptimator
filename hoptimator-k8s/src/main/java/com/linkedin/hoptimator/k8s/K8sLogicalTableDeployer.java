package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecPipelines;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import com.linkedin.hoptimator.util.DeploymentService;


/**
 * Deploys a logical table by:
 * <ol>
 *   <li>Creating physical resources for each tier (Kafka topic, Venice store, etc.) via
 *       the tier-specific deployer discovered through the SPI.</li>
 *   <li>Creating a {@code LogicalTable} CRD as the metadata record.</li>
 *   <li>Creating implicit inter-tier Pipeline CRDs owned by the LogicalTable CRD.</li>
 * </ol>
 *
 * <p>This class lives in {@code hoptimator-k8s} (not {@code hoptimator-logical}) because
 * it needs package-private access to {@link K8sPipelineDeployer} and
 * {@link K8sYamlDeployerImpl}.
 */
class K8sLogicalTableDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(K8sLogicalTableDeployer.class);

  private final Source source;
  private final K8sContext context;
  private final K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi;

  /** Deployers for tier resources created during {@link #create()}, used for rollback. */
  private final List<Deployer> tierDeployers = new ArrayList<>();

  /**
   * Tracks all Pipeline-related deployers created during {@link #create()}.
   * Used to restore() on rollback.
   */
  private final List<Deployer> pipelineDeployers = new ArrayList<>();

  K8sLogicalTableDeployer(Source source, K8sContext context) {
    this.source = source;
    this.context = context;
    this.logicalTableApi = new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES);
  }

  @Override
  public void create() throws SQLException {
    // 1. Parse tier map from source options (the logical DB URL)
    String logicalUrl = source.options().getOrDefault("url", "");
    Map<String, String> tierMap = parseTiers(logicalUrl);

    if (tierMap.isEmpty()) {
      throw new SQLException("No tiers defined in logical URL: " + logicalUrl);
    }

    // 2. For each tier: open connection, create tier resource
    Map<String, Connection> tierConnections = new LinkedHashMap<>();
    Map<String, V1alpha1Database> tierDatabases = new LinkedHashMap<>();
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<>(context, K8sApiEndpoints.DATABASES);

    try {
      for (Map.Entry<String, String> entry : tierMap.entrySet()) {
        String tierName = entry.getKey();
        String databaseCrdName = entry.getValue();

        V1alpha1Database dbCrd = dbApi.get(databaseCrdName);
        tierDatabases.put(tierName, dbCrd);

        String tierUrl = dbCrd.getSpec().getUrl();
        Connection tierConn = DriverManager.getConnection(tierUrl);
        tierConnections.put(tierName, tierConn);

        // Construct a Source for this tier: use the CRD name as the database
        Source tierSource = new Source(databaseCrdName, source.path(), source.options());
        Collection<Deployer> deployers = DeploymentService.deployers(tierSource, tierConn);
        for (Deployer deployer : deployers) {
          deployer.create();
          tierDeployers.add(deployer);
        }
      }
    } catch (Exception e) {
      // Rollback tier resources that were created before the failure
      rollbackTierDeployers();
      closeConnections(tierConnections);
      throw toSqlException("Failed to create tier resources", e);
    }

    // 3. Create the LogicalTable CRD
    String tableName = source.table();
    String crdName = K8sUtils.canonicalizeName(source.path());
    String avroSchema = source.options().getOrDefault("avroSchema", "");

    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      spec.putTiersItem(entry.getKey(),
          new V1alpha1LogicalTableSpecTiers().databaseCrdName(entry.getValue()));
    }
    spec.pipelines(new ArrayList<>());
    spec.avroSchema(avroSchema);

    V1alpha1LogicalTable logicalTable = new V1alpha1LogicalTable()
        .kind(K8sApiEndpoints.LOGICAL_TABLES.kind())
        .apiVersion(K8sApiEndpoints.LOGICAL_TABLES.apiVersion())
        .metadata(new V1ObjectMeta().name(crdName))
        .spec(spec);

    try {
      logicalTableApi.create(logicalTable);
    } catch (Exception e) {
      rollbackTierDeployers();
      closeConnections(tierConnections);
      throw toSqlException("Failed to create LogicalTable CRD", e);
    }

    // Get the owner reference for the just-created LogicalTable CRD
    V1OwnerReference logicalTableRef;
    try {
      logicalTableRef = logicalTableApi.reference(logicalTable);
    } catch (Exception e) {
      // If we can't get the reference, delete what we created and bail out
      try {
        logicalTableApi.delete(crdName);
      } catch (SQLException ignore) {
        // best-effort
      }
      rollbackTierDeployers();
      closeConnections(tierConnections);
      throw toSqlException("Failed to get LogicalTable CRD owner reference", e);
    }

    // 4. Create implicit pipelines based on which tiers are present
    List<V1alpha1LogicalTableSpecPipelines> pipelineRecords = new ArrayList<>();
    K8sContext logicalTableContext = context.withOwner(logicalTableRef);

    try {
      if (tierMap.containsKey("nearline") && tierMap.containsKey("online")) {
        V1alpha1LogicalTableSpecPipelines p = createImplicitPipeline(
            "nearline", "online", tableName, tierMap, tierDatabases, tierConnections,
            logicalTableContext);
        pipelineRecords.add(p);
      }

      if (tierMap.containsKey("nearline") && tierMap.containsKey("offline")) {
        log.info("Kyoto integration (nearline->offline) is pending; skipping pipeline creation "
            + "for table {}", tableName);
      }
    } catch (Exception e) {
      // Pipelines failed — restore pipeline deployers, delete LogicalTable (cascade handles owned
      // Pipeline CRDs), rollback tier deployers
      rollbackPipelineDeployers();
      try {
        logicalTableApi.delete(crdName);
      } catch (SQLException ignore) {
        // best-effort
      }
      rollbackTierDeployers();
      closeConnections(tierConnections);
      throw toSqlException("Failed to create implicit pipeline", e);
    }

    // 5. Update LogicalTable CRD with pipeline records
    if (!pipelineRecords.isEmpty()) {
      spec.pipelines(pipelineRecords);
      logicalTable.spec(spec);
      try {
        logicalTableApi.update(logicalTable);
      } catch (Exception e) {
        log.warn("Failed to update LogicalTable CRD with pipeline references; "
            + "table {} is functional but metadata is incomplete", crdName, e);
      }
    }

    closeConnections(tierConnections);
  }

  @Override
  public void update() throws SQLException {
    // Update is not yet implemented; re-create is the current strategy
    create();
  }

  @Override
  public void delete() throws SQLException {
    String crdName = K8sUtils.canonicalizeName(source.path());
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<>(context, K8sApiEndpoints.DATABASES);

    // Read the LogicalTable CRD to recover tier bindings
    V1alpha1LogicalTable logicalTable;
    try {
      logicalTable = logicalTableApi.get(crdName);
    } catch (SQLException e) {
      log.warn("LogicalTable CRD {} not found during delete; skipping tier cleanup", crdName);
      return;
    }

    if (logicalTable.getSpec() != null && logicalTable.getSpec().getTiers() != null) {
      for (Map.Entry<String, V1alpha1LogicalTableSpecTiers> entry
          : logicalTable.getSpec().getTiers().entrySet()) {
        String databaseCrdName = entry.getValue().getDatabaseCrdName();
        try {
          V1alpha1Database dbCrd = dbApi.get(databaseCrdName);
          Connection tierConn = DriverManager.getConnection(dbCrd.getSpec().getUrl());
          Source tierSource = new Source(databaseCrdName, source.path(), source.options());
          Collection<Deployer> deployers = DeploymentService.deployers(tierSource, tierConn);
          for (Deployer deployer : deployers) {
            deployer.delete();
          }
          tierConn.close();
        } catch (Exception e) {
          log.warn("Failed to delete tier {} resources for table {}: {}",
              entry.getKey(), crdName, e.getMessage());
        }
      }
    }

    // Delete the LogicalTable CRD; K8s cascade GC removes owned Pipeline CRDs
    logicalTableApi.delete(crdName);
  }

  @Override
  public void restore() {
    // Restore in reverse order:
    // 1. Pipeline deployers (delete Pipeline CRDs and their YAML children)
    rollbackPipelineDeployers();

    // 2. LogicalTable CRD (deletes it; K8s cascade handles any owned Pipeline CRDs)
    String crdName = K8sUtils.canonicalizeName(source.path());
    try {
      logicalTableApi.delete(crdName);
    } catch (SQLException e) {
      log.warn("Could not delete LogicalTable CRD {} during restore (may not exist): {}",
          crdName, e.getMessage());
    }

    // 3. Tier deployers in reverse order
    rollbackTierDeployers();
  }

  @Override
  public List<String> specify() throws SQLException {
    List<String> specs = new ArrayList<>();
    String logicalUrl = source.options().getOrDefault("url", "");
    Map<String, String> tierMap = parseTiers(logicalUrl);

    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<>(context, K8sApiEndpoints.DATABASES);

    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      String databaseCrdName = entry.getValue();
      try {
        V1alpha1Database dbCrd = dbApi.get(databaseCrdName);
        Connection tierConn = DriverManager.getConnection(dbCrd.getSpec().getUrl());
        Source tierSource = new Source(databaseCrdName, source.path(), source.options());
        specs.addAll(DeploymentService.specify(tierSource, tierConn));
        tierConn.close();
      } catch (Exception e) {
        log.warn("Failed to specify tier {} for source {}: {}", entry.getKey(), source, e.getMessage());
      }
    }
    return specs;
  }

  // --- Private helpers ---

  /**
   * Parses a {@code jdbc:logical://} URL into a tier map.
   *
   * <p>Mirrors {@code LogicalTableUrlParser.tiers()} from {@code hoptimator-logical}. The logic
   * is duplicated here to avoid a circular module dependency (hoptimator-k8s cannot depend on
   * hoptimator-logical because hoptimator-logical depends on hoptimator-k8s).
   */
  private Map<String, String> parseTiers(String url) {
    if (url == null || url.isEmpty()) {
      return Collections.emptyMap();
    }
    String prefix = "jdbc:logical://";
    if (!url.startsWith(prefix)) {
      log.warn("Not a logical URL (expected jdbc:logical://): {}", url);
      return Collections.emptyMap();
    }
    String params = url.substring(prefix.length());
    Map<String, String> tiers = new LinkedHashMap<>();
    for (String rawSegment : params.split(";")) {
      String segment = rawSegment.trim();
      if (segment.isEmpty()) {
        continue;
      }
      int eq = segment.indexOf('=');
      if (eq < 0) {
        continue;
      }
      String key = segment.substring(0, eq).trim();
      String value = segment.substring(eq + 1).trim();
      // Skip pipeline.* overrides; only tier keys go into the tiers map
      if (!key.startsWith("pipeline.")) {
        tiers.put(key, value);
      }
    }
    return Collections.unmodifiableMap(tiers);
  }

  /**
   * Creates a single implicit Pipeline CRD (fromTier to toTier) and returns the record for it.
   * The created pipeline deployers are tracked in {@link #pipelineDeployers} for rollback.
   */
  private V1alpha1LogicalTableSpecPipelines createImplicitPipeline(
      String fromTier,
      String toTier,
      String tableName,
      Map<String, String> tierMap,
      Map<String, V1alpha1Database> tierDatabases,
      Map<String, Connection> tierConnections,
      K8sContext ownerContext) throws SQLException {

    String fromCrdName = tierMap.get(fromTier);
    String toCrdName = tierMap.get(toTier);

    V1alpha1Database fromDb = tierDatabases.get(fromTier);
    V1alpha1Database toDb = tierDatabases.get(toTier);

    String sourceSchema = fromDb.getSpec().getSchema();
    String sinkSchema = toDb.getSpec().getSchema();

    Connection fromConn = tierConnections.get(fromTier);
    Connection toConn = tierConnections.get(toTier);

    // Get rendered YAML specs for the source and sink connectors
    Source fromSource = new Source(fromCrdName, source.path(), source.options());
    Source toSource = new Source(toCrdName, source.path(), source.options());

    List<String> pipelineSpecs = new ArrayList<>();
    pipelineSpecs.addAll(DeploymentService.specify(fromSource, fromConn));
    pipelineSpecs.addAll(DeploymentService.specify(toSource, toConn));

    // Generate SQL: INSERT INTO `sinkSchema`.`table` SELECT * FROM `sourceSchema`.`table`
    String sql = buildPipelineSql(sourceSchema, sinkSchema, tableName);

    // Pipeline name uses double underscore prefix to avoid accidental user collisions
    // K8s names must be [a-z0-9-]+ — "logical-" prefix distinguishes system-managed pipelines.
    String pipelineName = "logical-" + K8sUtils.canonicalizeName(tableName) + "-" + fromTier + "-to-" + toTier;

    // Use K8sPipelineDeployer directly (package-private) to create the Pipeline CRD
    // owned by the LogicalTable CRD. Then create the YAML child resources.
    K8sPipelineDeployer pipelineDeployer = new K8sPipelineDeployer(pipelineName, pipelineSpecs, sql, ownerContext);
    pipelineDeployers.add(pipelineDeployer);
    V1OwnerReference pipelineRef = pipelineDeployer.createAndReference();

    K8sContext pipelineContext = ownerContext.withLabel("pipeline", pipelineName).withOwner(pipelineRef);
    K8sYamlDeployerImpl yamlDeployer = new K8sYamlDeployerImpl(pipelineContext, pipelineSpecs);
    pipelineDeployers.add(yamlDeployer);
    yamlDeployer.update();  // update, since some elements may already exist

    return new V1alpha1LogicalTableSpecPipelines()
        .name(pipelineName)
        .pipelineCrdName(pipelineName)
        .fromTier(fromTier)
        .toTier(toTier);
  }

  private static String buildPipelineSql(String sourceSchema, String sinkSchema, String tableName) {
    return String.format(
        "INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`",
        sinkSchema, tableName, sourceSchema, tableName);
  }

  private void rollbackTierDeployers() {
    for (int i = tierDeployers.size() - 1; i >= 0; i--) {
      tierDeployers.get(i).restore();
    }
  }

  private void rollbackPipelineDeployers() {
    for (int i = pipelineDeployers.size() - 1; i >= 0; i--) {
      pipelineDeployers.get(i).restore();
    }
  }

  private static void closeConnections(Map<String, Connection> connections) {
    for (Map.Entry<String, Connection> entry : connections.entrySet()) {
      try {
        entry.getValue().close();
      } catch (SQLException e) {
        log.warn("Error closing connection for tier {}: {}", entry.getKey(), e.getMessage());
      }
    }
  }

  private static SQLException toSqlException(String message, Exception cause) {
    if (cause instanceof SQLException) {
      return (SQLException) cause;
    }
    SQLException e = new SQLException(message + ": " + cause.getMessage());
    e.initCause(cause);
    return e;
  }
}

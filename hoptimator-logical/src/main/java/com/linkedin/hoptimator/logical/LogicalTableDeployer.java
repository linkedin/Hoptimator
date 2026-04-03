package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.util.Properties;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.HashSet;
import java.util.Set;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sPipelineCreator;
import com.linkedin.hoptimator.k8s.K8sUtils;


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
public class LogicalTableDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployer.class);

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

  private final Properties tierProps;

  LogicalTableDeployer(Source source, Properties tierProps, K8sContext context) {
    this.source = source;
    this.tierProps = tierProps;
    this.context = context;
    this.logicalTableApi = new K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList>(context, K8sApiEndpoints.LOGICAL_TABLES);
  }

  @Override
  public void create() throws SQLException {
    // 1. Build tier map from injected Properties (parsed from the JDBC URL by the provider).
    // Keys are tier names (e.g. "nearline", "online"), values are Database CRD names.
    Map<String, String> tierMap = new LinkedHashMap<>();
    for (String key : tierProps.stringPropertyNames()) {
      if (!key.startsWith("pipeline.")) {
        tierMap.put(key, tierProps.getProperty(key));
      }
    }

    if (tierMap.isEmpty()) {
      throw new SQLException("No tiers defined in logical database properties for table " + source.table());
    }

    // 2. For each tier: open connection, create tier resource
    Map<String, Connection> tierConnections = new LinkedHashMap<>();
    Map<String, V1alpha1Database> tierDatabases = new LinkedHashMap<>();
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(context, K8sApiEndpoints.DATABASES);

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

    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      spec.putTiersItem(entry.getKey(),
          new V1alpha1LogicalTableSpecTiers().databaseCrdName(entry.getValue()));
    }
    spec.pipelines(new ArrayList<>());

    // Label the CRD with the schema name so LogicalTableSchema can filter by database.
    String schemaLabel = source.schema() != null ? source.schema() : "LOGICAL";
    V1alpha1LogicalTable logicalTable = new V1alpha1LogicalTable()
        .kind(K8sApiEndpoints.LOGICAL_TABLES.kind())
        .apiVersion(K8sApiEndpoints.LOGICAL_TABLES.apiVersion())
        .metadata(new V1ObjectMeta().name(crdName)
            .putLabelsItem(LogicalTableSchema.SCHEMA_LABEL, schemaLabel))
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
      } catch (SQLException deleteEx) {
        log.warn("Failed to delete LogicalTable CRD {} during rollback: {}", crdName, deleteEx.getMessage());
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
      } catch (SQLException deleteEx) {
        log.warn("Failed to delete LogicalTable CRD {} during pipeline rollback: {}", crdName, deleteEx.getMessage());
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
    // Parse tier map from injected Properties.
    Map<String, String> tierMap = new LinkedHashMap<>();
    for (String key : tierProps.stringPropertyNames()) {
      if (!key.startsWith("pipeline.")) {
        tierMap.put(key, tierProps.getProperty(key));
      }
    }
    if (tierMap.isEmpty()) {
      throw new SQLException("No tiers defined in logical database properties for table " + source.table());
    }

    // Update physical tier resources (compatibility checks happen inside each tier deployer).
    Map<String, Connection> tierConnections = new LinkedHashMap<>();
    Map<String, V1alpha1Database> tierDatabases = new LinkedHashMap<>();
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(context, K8sApiEndpoints.DATABASES);

    try {
      for (Map.Entry<String, String> entry : tierMap.entrySet()) {
        String tierName = entry.getKey();
        String databaseCrdName = entry.getValue();
        log.info("Updating tier {} (database CRD: {}) for table {}", tierName, databaseCrdName, source.table());

        V1alpha1Database dbCrd = dbApi.get(databaseCrdName);
        tierDatabases.put(tierName, dbCrd);
        Connection tierConn = DriverManager.getConnection(dbCrd.getSpec().getUrl());
        tierConnections.put(tierName, tierConn);

        Source tierSource = new Source(databaseCrdName, source.path(), source.options());
        Collection<Deployer> deployers = DeploymentService.deployers(tierSource, tierConn);
        for (Deployer deployer : deployers) {
          deployer.update();
          tierDeployers.add(deployer);
        }
      }
    } catch (Exception e) {
      rollbackTierDeployers();
      closeConnections(tierConnections);
      throw toSqlException("Failed to update tier resources", e);
    }

    // Update implicit pipelines via K8sPipelineCreator.update() which calls
    // K8sPipelineDeployer.updateAndReference() — same as K8sMaterializedViewDeployer.updatePipelineWithOwner().
    String tableName = source.table();
    String crdName = K8sUtils.canonicalizeName(source.path());

    // Re-read the existing LogicalTable CRD to get its owner reference.
    V1alpha1LogicalTable existingCrd;
    try {
      existingCrd = logicalTableApi.get(crdName);
    } catch (SQLException e) {
      log.warn("LogicalTable CRD {} not found during update; treating as create", crdName);
      create();
      return;
    }

    K8sContext ownerContext = context.withOwner(logicalTableApi.reference(existingCrd));

    try {
      if (tierMap.containsKey("nearline") && tierMap.containsKey("online")) {
        String pipelineName = "logical-" + K8sUtils.canonicalizeName(tableName) + "-nearline-to-online";
        List<String> pipelineSpecs = buildPipelineSpecs("nearline", "online",
            tierMap, tierDatabases, tierConnections);
        String sql = buildPipelineSql(
            tierDatabases.get("nearline").getSpec().getSchema(),
            tierDatabases.get("online").getSpec().getSchema(),
            tableName);
        K8sPipelineCreator creator = new K8sPipelineCreator(pipelineName, pipelineSpecs, sql, ownerContext);
        pipelineDeployers.add(creator);
        creator.update();
      }
      if (tierMap.containsKey("nearline") && tierMap.containsKey("offline")) {
        log.info("Kyoto integration (nearline->offline) pending; skipping pipeline update for {}", tableName);
      }
    } catch (Exception e) {
      rollbackPipelineDeployers();
      rollbackTierDeployers();
      closeConnections(tierConnections);
      throw toSqlException("Failed to update implicit pipeline", e);
    }

    closeConnections(tierConnections);
  }

  @Override
  public void delete() throws SQLException {
    // TODO: Implement safe logical table deletion.
    // Deletion is not yet supported because we cannot guarantee the logical table
    // is not currently referenced by an existing pipeline (e.g. a CREATE MATERIALIZED VIEW
    // that reads from this table). Deleting physical tier resources under an active pipeline
    // would cause data loss and pipeline failures.
    // Resolution: check for dependent pipelines in the K8s namespace before allowing deletion,
    // and block or warn if any are found.
    throw new SQLFeatureNotSupportedException(
        "DROP TABLE on logical tables is not yet supported. "
        + "Cannot safely delete physical tier resources without verifying no active pipelines "
        + "depend on this table. See TODO in LogicalTableDeployer.delete().");
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
    Map<String, String> tierMap = new LinkedHashMap<>();
    for (String key : tierProps.stringPropertyNames()) {
      if (!key.startsWith("pipeline.")) {
        tierMap.put(key, tierProps.getProperty(key));
      }
    }

    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(context, K8sApiEndpoints.DATABASES);

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

    String sql = buildPipelineSql(sourceSchema, sinkSchema, tableName);
    String pipelineName = "logical-" + K8sUtils.canonicalizeName(tableName) + "-" + fromTier + "-to-" + toTier;

    // Build source + sink connector YAML specs from TableTemplates.
    List<String> pipelineSpecs = new ArrayList<>();
    pipelineSpecs.addAll(DeploymentService.specify(fromSource, fromConn));
    pipelineSpecs.addAll(DeploymentService.specify(toSource, toConn));

    // Build job YAML specs from JobTemplates (e.g. FlinkSessionJob) for the sink tier.
    // The Job object carries the SQL and sink info needed for JobTemplate rendering.
    Sink sinkObj = new Sink(toCrdName, source.path(), source.options());
    Set<Source> sourcesSet = new HashSet<>();
    sourcesSet.add(fromSource);
    ThrowingFunction<SqlDialect, String> sqlFn = dialect -> sql;
    ThrowingFunction<SqlDialect, String> queryFn =
        dialect -> "SELECT * FROM `" + sourceSchema + "`.`" + tableName + "`";
    ThrowingFunction<SqlDialect, String> fieldMapFn = dialect -> "{}";
    Job job = new Job(pipelineName, sourcesSet, sinkObj,
        java.util.Map.of("sql", sqlFn, "query", queryFn, "fieldMap", fieldMapFn));
    pipelineSpecs.addAll(DeploymentService.specify(job, context.connection()));

    K8sPipelineCreator pipelineCreator = new K8sPipelineCreator(pipelineName, pipelineSpecs, sql, ownerContext);
    pipelineDeployers.add(pipelineCreator);
    pipelineCreator.create();

    return new V1alpha1LogicalTableSpecPipelines()
        .name(pipelineName)
        .pipelineCrdName(pipelineName)
        .fromTier(fromTier)
        .toTier(toTier);
  }

  private List<String> buildPipelineSpecs(String fromTier, String toTier,
      Map<String, String> tierMap, Map<String, V1alpha1Database> tierDatabases,
      Map<String, Connection> tierConnections) throws SQLException {
    String fromCrdName = tierMap.get(fromTier);
    String toCrdName = tierMap.get(toTier);
    Connection fromConn = tierConnections.get(fromTier);
    Connection toConn = tierConnections.get(toTier);
    Source fromSource = new Source(fromCrdName, source.path(), source.options());
    Source toSource = new Source(toCrdName, source.path(), source.options());
    List<String> specs = new ArrayList<>();
    specs.addAll(DeploymentService.specify(fromSource, fromConn));
    specs.addAll(DeploymentService.specify(toSource, toConn));
    return specs;
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

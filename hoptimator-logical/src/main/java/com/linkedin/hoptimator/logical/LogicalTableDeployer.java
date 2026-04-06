package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sPipelineBundle;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import com.linkedin.hoptimator.util.DeploymentService;


/**
 * Deploys a logical table by:
 * <ol>
 *   <li>Validating that at least two tiers are defined.</li>
 *   <li>Creating physical resources for each tier via the tier-specific deployer SPI.</li>
 *   <li>Creating a {@code LogicalTable} CRD as the metadata record.</li>
 *   <li>Creating implicit inter-tier Pipeline CRDs owned by the LogicalTable CRD.</li>
 * </ol>
 */
public class LogicalTableDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableDeployer.class);

  /**
   * The set of tier names that this deployer recognises in the JDBC URL properties.
   * Only these keys are treated as tier declarations; all other properties are ignored.
   */
  static final Set<String> SUPPORTED_TIERS = Set.of("nearline", "offline", "online");

  private final Source source;
  private final Properties tierProps;
  private final K8sContext context;
  private final K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi;

  private final List<Deployer> tierDeployers = new ArrayList<>();
  private final List<K8sPipelineBundle> pipelineDeployers = new ArrayList<>();

  LogicalTableDeployer(Source source, Properties tierProps, K8sContext context) {
    this.source = source;
    this.tierProps = tierProps;
    this.context = context;
    this.logicalTableApi = new K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList>(
        context, K8sApiEndpoints.LOGICAL_TABLES);
  }

  @Override
  public void create() throws SQLException {
    Map<String, String> tierMap = buildTierMap();
    Map<String, Connection> tierConnections = openTierConnections(tierMap);
    try {
      Map<String, V1alpha1Database> tierDatabases = resolveTierDatabases(tierMap);
      deployTierResources(tierMap, tierDatabases, tierConnections, false);
      V1OwnerReference ownerRef = createLogicalTableCrd(tierMap);
      deployImplicitPipelines(tierMap, tierDatabases, tierConnections,
          context.withOwner(ownerRef), false);
    } catch (Exception e) {
      restore();
      throw toSqlException("Failed to create logical table " + source.table(), e);
    } finally {
      closeConnections(tierConnections);
    }
  }

  @Override
  public void update() throws SQLException {
    Map<String, String> tierMap = buildTierMap();
    Map<String, Connection> tierConnections = openTierConnections(tierMap);
    try {
      Map<String, V1alpha1Database> tierDatabases = resolveTierDatabases(tierMap);
      deployTierResources(tierMap, tierDatabases, tierConnections, true);

      // Re-read the existing LogicalTable CRD to get its owner reference for pipeline updates.
      String crdName = K8sUtils.canonicalizeName(source.path());
      V1alpha1LogicalTable existingCrd;
      try {
        existingCrd = logicalTableApi.get(crdName);
      } catch (SQLException e) {
        log.warn("LogicalTable CRD {} not found during update; falling back to create", crdName);
        create();
        return;
      }
      deployImplicitPipelines(tierMap, tierDatabases, tierConnections,
          context.withOwner(logicalTableApi.reference(existingCrd)), true);
    } catch (Exception e) {
      restore();
      throw toSqlException("Failed to update logical table " + source.table(), e);
    } finally {
      closeConnections(tierConnections);
    }
  }

  @Override
  public void delete() throws SQLException {
    // TODO: Implement safe logical table deletion.
    // Deletion is blocked until we can verify no active pipelines depend on this table.
    // See: LogicalTableDeployer.delete()
    throw new SQLFeatureNotSupportedException(
        "DROP TABLE on logical tables is not yet supported. "
        + "Cannot safely delete physical tier resources without verifying no active pipelines "
        + "depend on this table.");
  }

  @Override
  public void restore() {
    // Restore pipeline deployers first (they reference the LogicalTable CRD via ownerRef)
    for (int i = pipelineDeployers.size() - 1; i >= 0; i--) {
      pipelineDeployers.get(i).restore();
    }
    // Delete the LogicalTable CRD; K8s cascade GC removes owned Pipeline CRDs
    String crdName = K8sUtils.canonicalizeName(source.path());
    try {
      logicalTableApi.delete(crdName);
    } catch (SQLException e) {
      log.warn("Could not delete LogicalTable CRD {} during restore (may not exist): {}",
          crdName, e.getMessage());
    }
    // Restore tier deployers in reverse
    for (int i = tierDeployers.size() - 1; i >= 0; i--) {
      tierDeployers.get(i).restore();
    }
  }

  // ─── Private helpers ────────────────────────────────────────────────────────

  /** Builds the tier map from the connection properties, restricted to {@link #SUPPORTED_TIERS}. */
  private Map<String, String> buildTierMap() throws SQLException {
    Map<String, String> tierMap = new LinkedHashMap<>();
    for (String tier : SUPPORTED_TIERS) {
      String val = tierProps.getProperty(tier);
      if (val != null && !val.isEmpty()) {
        tierMap.put(tier, val);
      }
    }
    if (tierMap.size() < 2) {
      throw new SQLException("A logical table must span at least 2 tiers, but only "
          + tierMap.size() + " tier(s) were defined for table " + source.table()
          + ". Define at least two tiers (e.g. nearline and online) in the Database CRD URL.");
    }
    return tierMap;
  }

  /** Opens a JDBC connection to each tier's Database CRD URL. */
  private Map<String, Connection> openTierConnections(Map<String, String> tierMap)
      throws SQLException {
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(context, K8sApiEndpoints.DATABASES);
    Map<String, Connection> connections = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      V1alpha1Database dbCrd = dbApi.get(entry.getValue());
      connections.put(entry.getKey(), DriverManager.getConnection(dbCrd.getSpec().getUrl()));
    }
    return connections;
  }

  /** Reads all tier Database CRDs from K8s. */
  private Map<String, V1alpha1Database> resolveTierDatabases(Map<String, String> tierMap)
      throws SQLException {
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(context, K8sApiEndpoints.DATABASES);
    Map<String, V1alpha1Database> databases = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      databases.put(entry.getKey(), dbApi.get(entry.getValue()));
    }
    return databases;
  }

  /**
   * Creates or updates physical resources for each tier using the deployer SPI.
   * Created deployers are tracked in {@link #tierDeployers} for rollback.
   */
  private void deployTierResources(Map<String, String> tierMap,
      Map<String, V1alpha1Database> tierDatabases, Map<String, Connection> tierConnections,
      boolean update) throws SQLException {
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      String tierName = entry.getKey();
      String databaseCrdName = entry.getValue();
      log.info("{} tier {} (database CRD: {}) for table {}",
          update ? "Updating" : "Creating", tierName, databaseCrdName, source.table());
      Connection tierConn = tierConnections.get(tierName);
      Source tierSource = new Source(databaseCrdName, source.path(), source.options());
      Collection<Deployer> deployers = DeploymentService.deployers(tierSource, tierConn);
      for (Deployer deployer : deployers) {
        if (update) {
          deployer.update();
        } else {
          deployer.create();
        }
        tierDeployers.add(deployer);
      }
    }
  }

  /** Creates the LogicalTable CRD and returns its owner reference. */
  private V1OwnerReference createLogicalTableCrd(Map<String, String> tierMap) throws SQLException {
    String crdName = K8sUtils.canonicalizeName(source.path());
    String schemaLabel = source.schema() != null ? source.schema() : "LOGICAL";

    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      spec.putTiersItem(entry.getKey(),
          new V1alpha1LogicalTableSpecTiers().databaseCrdName(entry.getValue()));
    }
    spec.pipelines(new ArrayList<>());

    V1alpha1LogicalTable crd = new V1alpha1LogicalTable()
        .kind(K8sApiEndpoints.LOGICAL_TABLES.kind())
        .apiVersion(K8sApiEndpoints.LOGICAL_TABLES.apiVersion())
        .metadata(new V1ObjectMeta().name(crdName)
            .putLabelsItem(LogicalTableSchema.SCHEMA_LABEL, schemaLabel))
        .spec(spec);

    logicalTableApi.create(crd);
    return logicalTableApi.reference(crd);
  }

  /**
   * Creates or updates implicit inter-tier pipeline CRDs.
   * Each created bundle is tracked in {@link #pipelineDeployers}.
   */
  private void deployImplicitPipelines(Map<String, String> tierMap,
      Map<String, V1alpha1Database> tierDatabases, Map<String, Connection> tierConnections,
      K8sContext ownerContext, boolean update) throws SQLException {
    if (tierMap.containsKey("nearline") && tierMap.containsKey("online")) {
      deployPipelineBundle("nearline", "online", tierMap, tierDatabases, tierConnections,
          ownerContext, update);
    }
    if (tierMap.containsKey("nearline") && tierMap.containsKey("offline")) {
      log.info("Kyoto integration (nearline→offline) pending; skipping pipeline for {}",
          source.table());
    }
  }

  /**
   * Deploys a single implicit pipeline between two tiers using the full pipeline planner,
   * producing a proper {@link Job} with correct SQL, fieldMap, and connector configs.
   */
  private void deployPipelineBundle(String fromTier, String toTier,
      Map<String, String> tierMap, Map<String, V1alpha1Database> tierDatabases,
      Map<String, Connection> tierConnections, K8sContext ownerContext,
      boolean update) throws SQLException {
    String tableName = source.table();
    String pipelineName = pipelineName(tableName, fromTier, toTier);

    String fromCrdName = tierMap.get(fromTier);
    String toCrdName = tierMap.get(toTier);
    String sourceSchema = tierDatabases.get(fromTier).getSpec().getSchema();
    String sinkSchema = tierDatabases.get(toTier).getSpec().getSchema();
    Connection fromConn = tierConnections.get(fromTier);
    Connection toConn = tierConnections.get(toTier);

    // Build INSERT INTO ... SELECT ... SQL for the pipeline planner.
    String pipelineSql = buildPipelineSql(sourceSchema, sinkSchema, tableName);

    // Use the pipeline planner (PipelineRel) to generate a proper Job with full SQL,
    // fieldMap, and connector configs — same approach as HoptimatorDdlExecutor.
    List<String> pipelineSpecs = new ArrayList<>();
    Source fromSource = new Source(fromCrdName, source.path(), source.options());
    Source toSource = new Source(toCrdName, source.path(), source.options());
    pipelineSpecs.addAll(DeploymentService.specify(fromSource, fromConn));
    pipelineSpecs.addAll(DeploymentService.specify(toSource, toConn));

    // Plan the pipeline SQL to get a proper Job with full SQL generation.
    // context.connection() always returns HoptimatorConnection.
    try {
      HoptimatorConnection hConn = context.connection();
        Properties props = hConn.connectionProperties();
        props.setProperty(DeploymentService.PIPELINE_OPTION, pipelineName);
        org.apache.calcite.rel.RelRoot root =
            HoptimatorDriver.convert(hConn, pipelineSql).root;
        com.linkedin.hoptimator.util.planner.PipelineRel.Implementor plan =
            DeploymentService.plan(root, hConn.materializations(), props);
        Pipeline pipeline = plan.pipeline(pipelineName, hConn);
        Job job = pipeline.job();
        pipelineSpecs.addAll(DeploymentService.specify(job, hConn));
    } catch (Exception e) {
      log.warn("Pipeline planner failed for {}->{} on table {}; proceeding without job specs: {}",
          fromTier, toTier, tableName, e.getMessage());
    }

    K8sPipelineBundle bundle = new K8sPipelineBundle(pipelineName, pipelineSpecs,
        pipelineSql, ownerContext);
    pipelineDeployers.add(bundle);
    if (update) {
      bundle.update();
    } else {
      bundle.create();
    }
  }

  /** Returns the canonical pipeline name for an implicit inter-tier pipeline. */
  static String pipelineName(String tableName, String fromTier, String toTier) {
    return "logical-" + K8sUtils.canonicalizeName(tableName) + "-" + fromTier + "-to-" + toTier;
  }

  private static String buildPipelineSql(String sourceSchema, String sinkSchema, String table) {
    return String.format("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`",
        sinkSchema, table, sourceSchema, table);
  }

  private static void closeConnections(Map<String, Connection> connections) {
    for (Map.Entry<String, Connection> entry : connections.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Exception e) {
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

  @Override
  public List<String> specify() {
    // LogicalTableDeployer does not expose specify() — tier deployers manage their own specs.
    return List.of();
  }
}

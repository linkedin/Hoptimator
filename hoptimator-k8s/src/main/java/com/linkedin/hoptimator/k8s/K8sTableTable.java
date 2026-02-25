package com.linkedin.hoptimator.k8s;

import java.util.Objects;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.linkedin.hoptimator.k8s.models.V1alpha1Table;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableStatus;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;


/** System table that lists Table CRDs and registers them in the Calcite schema. */
class K8sTableTable extends K8sTable<V1alpha1Table, V1alpha1TableList, K8sTableTable.Row>
    implements Validated {

  // CHECKSTYLE:OFF
  public static class Row {
    public final String NAME;
    public final String CATALOG;
    public final String SCHEMA;
    public final String TABLE;
    public final String DATABASE;
    public final Boolean READY;

    public Row(String name, String catalog, String schema, String table, String database, Boolean ready) {
      this.NAME = name;
      this.CATALOG = catalog;
      this.SCHEMA = schema;
      this.TABLE = table;
      this.DATABASE = database;
      this.READY = ready;
    }

    /** The schema path components (catalog, schema) leading up to the table name. */
    java.util.List<String> schemaPath() {
      java.util.List<String> path = new java.util.ArrayList<>();
      if (CATALOG != null && !CATALOG.isEmpty()) {
        path.add(CATALOG);
      }
      if (SCHEMA != null && !SCHEMA.isEmpty()) {
        path.add(SCHEMA);
      }
      return path;
    }

    String tableName() {
      return TABLE;
    }

    @Override
    public String toString() {
      return String.join("\t", NAME, CATALOG, SCHEMA, TABLE, DATABASE,
          Boolean.toString(READY != null && READY));
    }
  }
  // CHECKSTYLE:ON

  private final K8sContext context;

  K8sTableTable(K8sContext context) {
    super(context, K8sApiEndpoints.TABLES, Row.class);
    this.context = context;
  }

  /** Loads Table CRs and registers them as K8sStoredTable instances in the schema hierarchy. */
  public void addTables(SchemaPlus parentSchema) {
    K8sApi<V1alpha1Table, V1alpha1TableList> api = new K8sApi<>(context, K8sApiEndpoints.TABLES);
    try {
      for (V1alpha1Table table : api.list()) {
        V1alpha1TableSpec spec = table.getSpec();
        if (spec == null || spec.getTable() == null) {
          continue;
        }

        // Build schema path, filling in any missing schemas
        SchemaPlus schema = parentSchema;
        if (spec.getCatalog() != null && !spec.getCatalog().isEmpty()) {
          SchemaPlus next = Objects.requireNonNull(schema).subSchemas().get(spec.getCatalog());
          if (next == null) {
            schema.add(spec.getCatalog(), new AbstractSchema());
            next = schema.subSchemas().get(spec.getCatalog());
          }
          schema = next;
        }
        if (spec.getSchema() != null && !spec.getSchema().isEmpty()) {
          SchemaPlus next = Objects.requireNonNull(schema).subSchemas().get(spec.getSchema());
          if (next == null) {
            schema.add(spec.getSchema(), new AbstractSchema());
            next = schema.subSchemas().get(spec.getSchema());
          }
          schema = next;
        }

        K8sStoredTable storedTable = new K8sStoredTable(
            spec.getDatabase(),
            spec.getColumns(),
            spec.getOptions());
        Objects.requireNonNull(schema).add(spec.getTable(), storedTable);
      }
    } catch (RuntimeException e) {
      // If Tables CRD is not installed, silently skip
    } catch (java.sql.SQLException e) {
      // If Tables CRD is not installed, silently skip
    }
  }

  @Override
  public Row toRow(V1alpha1Table obj) {
    V1alpha1TableSpec spec = Objects.requireNonNull(obj.getSpec());
    V1alpha1TableStatus status = obj.getStatus();
    return new Row(
        Objects.requireNonNull(obj.getMetadata()).getName(),
        spec.getCatalog(),
        spec.getSchema(),
        spec.getTable(),
        spec.getDatabase(),
        status != null ? status.getReady() : null);
  }

  @Override
  public V1alpha1Table fromRow(Row row) {
    K8sUtils.checkK8sName(row.NAME);
    return new V1alpha1Table()
        .kind(K8sApiEndpoints.TABLES.kind())
        .apiVersion(K8sApiEndpoints.TABLES.apiVersion())
        .metadata(new V1ObjectMeta().name(row.NAME))
        .spec(new V1alpha1TableSpec()
            .table(row.TABLE)
            .database(row.DATABASE)
            .catalog(row.CATALOG)
            .schema(row.SCHEMA));
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Override
  public void validate(Validator.Issues issues) {
    for (Row row : rows()) {
      Validator.Issues issues2 = issues.child(row.toString());
      Validator.validateSubdomainName(row.NAME, issues2.child("NAME"));
    }
    Validator.validateUnique(rows(), x -> x.NAME, issues);
  }
}

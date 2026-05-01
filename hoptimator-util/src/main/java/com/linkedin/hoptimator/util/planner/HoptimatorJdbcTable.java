package com.linkedin.hoptimator.util.planner;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.hoptimator.avro.AvroSchemaSource;
import com.linkedin.hoptimator.util.DataTypeUtils;
import org.apache.avro.Schema;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;


public class HoptimatorJdbcTable extends AbstractQueryableTable implements TranslatableTable,
    ModifiableTable, AvroSchemaSource {

  private final JdbcTable jdbcTable;
  private final HoptimatorJdbcConvention convention;

  public HoptimatorJdbcTable(JdbcTable jdbcTable, HoptimatorJdbcConvention convention) {
    super(Object[].class);
    this.jdbcTable = jdbcTable;
    this.convention = convention;
  }

  public JdbcTable jdbcTable() {
    return jdbcTable;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory factory) {
    return DataTypeUtils.unflatten(jdbcTable.getRowType(factory), factory);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new HoptimatorJdbcTableScan(context.getCluster(), context.getTableHints(), relOptTable, this,
        convention);
  }

  @Override
  public TableModify toModificationRel(RelOptCluster cluster,
      RelOptTable table, CatalogReader catalogReader, RelNode input,
      TableModify.Operation operation, List<String> updateColumnList,
      List<RexNode> sourceExpressionList, boolean flattened) {
    return jdbcTable.toModificationRel(cluster, table, catalogReader, input, operation,
        updateColumnList, sourceExpressionList, flattened);
  }

  @Override
  public Collection getModifiableCollection() {
    return null;
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return jdbcTable.asQueryable(queryProvider, schema, tableName);
  }

  /**
   * Returns the upstream's native Avro value schema when the underlying data source exposes one
   * via {@link AvroSchemaSource}. Returns {@code null} when the upstream is not a Calcite
   * connection, when the upstream table cannot be located, or when it does not implement the
   * interface.
   *
   * <p>Opens a new upstream JDBC connection on each call; callers should cache if invoked
   * repeatedly.
   */
  @Override
  public @Nullable Schema valueSchema() {
    Table upstream = upstreamTable();
    return upstream instanceof AvroSchemaSource ? ((AvroSchemaSource) upstream).valueSchema() : null;
  }

  /**
   * Returns the upstream's native Avro key schema when present, or {@code null} when the upstream
   * table has no distinct key concept or does not implement {@link AvroSchemaSource}.
   */
  @Override
  public @Nullable Schema keySchema() {
    Table upstream = upstreamTable();
    return upstream instanceof AvroSchemaSource ? ((AvroSchemaSource) upstream).keySchema() : null;
  }

  @VisibleForTesting
  @Nullable Table upstreamTable() {
    try (Connection upstream = jdbcTable.jdbcSchema.getDataSource().getConnection()) {
      CalciteConnection calciteUpstream;
      try {
        calciteUpstream = upstream.unwrap(CalciteConnection.class);
      } catch (SQLException e) {
        return null;
      }
      SchemaPlus schema = calciteUpstream.getRootSchema();
      if (schema == null) {
        return null;
      }
      if (jdbcTable.jdbcSchemaName != null) {
        schema = schema.subSchemas().get(jdbcTable.jdbcSchemaName);
        if (schema == null) {
          return null;
        }
      }
      return schema.tables().get(jdbcTable.jdbcTableName);
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to open upstream connection while reading Avro schema for " + jdbcTable.jdbcTableName, e);
    }
  }
}

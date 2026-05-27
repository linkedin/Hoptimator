/*
 * N.B. much of this code is copy-pasted from the base class in
 * upstream Apache Calcite.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.UserJob;
import com.linkedin.hoptimator.View;
import com.linkedin.hoptimator.jdbc.ddl.HoptimatorDdlParserImpl;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateDatabase;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTable;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlDropTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlFireTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlPauseTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlResumeTrigger;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcTable;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;

import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class HoptimatorDdlExecutor extends ServerDdlExecutor {

  private final HoptimatorConnection connection;
  private final HoptimatorConnection.HoptimatorConnectionDualLogger logger;

  public HoptimatorDdlExecutor(HoptimatorConnection connection) {
    try {
      if (connection.isReadOnly()) {
        throw new DdlException("Cannot execute DDL in read-only mode");
      }
    } catch (SQLException e) {
      throw new DdlException("Failed to check read-only mode", e);
    }
    this.connection = connection;
    this.logger = connection.getLogger(HoptimatorDdlExecutor.class);
  }

  public static final SqlParserImplFactory PARSER_FACTORY = new SqlParserImplFactory() {
    @Override
    public SqlAbstractParserImpl getParser(Reader stream) {
      SqlAbstractParserImpl parser = HoptimatorDdlParserImpl.FACTORY.getParser(stream);
      parser.setConformance(SqlConformanceEnum.BABEL);
      return parser;
    }

    @Override
    public DdlExecutor getDdlExecutor() {
      return HoptimatorDdlExecutor.INSTANCE;
    }
  };

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes a {@code CREATE VIEW} command. */
  @Override
  public void execute(SqlCreateView create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    try {
      ValidationService.validateOrThrow(create, connection);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, true, create.name);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.name + " not found.");
    }
    final SchemaPlus schemaPlus = pair.left.plus();
    if (schemaPlus.tables().get(pair.right) instanceof HoptimatorJdbcTable) {
      throw new DdlException(create,
          "Cannot overwrite physical table " + pair.right + " with a view.");
    }
    for (Function function : schemaPlus.getFunctions(pair.right)) {
      if (function.getParameters().isEmpty()) {
        if (!create.getReplace()) {
          throw new DdlException(create,
              "View " + pair.right + " already exists. Use CREATE OR REPLACE to update.");
        }
        pair.left.removeFunction(pair.right);
      }
    }

    final SqlNode q = HoptimatorDdlUtils.renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final List<String> schemaPath = pair.left.path(null);
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);
    ViewTable viewTable = HoptimatorDdlUtils.viewTable(context, sql, new HoptimatorDriver.Prepare(connection), schemaPath, viewPath);
    View view = new View(viewPath, sql);
    logger.info("Validated sql statement. The view is named {} and has path {}",
        viewName, viewPath);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Validating deployable resources for view {}", viewName);
      ValidationService.validateOrThrow(viewTable, connection);
      deployers = DeploymentService.deployers(view, connection);
      ValidationService.validateOrThrow(deployers, connection);
      logger.info("Validated view {}", viewName);
      if (create.getReplace()) {
        logger.info("Deploying update view {}", viewName);
        DeploymentService.update(deployers);
      } else {
        logger.info("Deploying create view {}", viewName);
        DeploymentService.create(deployers);
      }
      logger.info("Deployed view {}", viewName);
      schemaPlus.add(viewName, viewTable);
      logger.info("Added view {} to schema {}", viewName, schemaPlus.getName());
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy view {}", viewName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for view {}", viewName);
      }
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE VIEW {} completed", viewName);
  }

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes a {@code CREATE MATERIALIZED VIEW} command. */
  public void execute(SqlCreateMaterializedView create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    HoptimatorDdlUtils.DdlMode mode = create.getReplace()
        ? HoptimatorDdlUtils.DdlMode.UPDATE : HoptimatorDdlUtils.DdlMode.CREATE;
    try {
      HoptimatorDdlUtils.processCreateMaterializedView(
          context,
          new HoptimatorDriver.Prepare(connection),
          connection,
          create,
          mode);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy materialized view {}", create.name);
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE MATERIALIZED VIEW {} completed", create.name);
  }

  /** Executes a {@code CREATE TRIGGER} command. */
  public void execute(SqlCreateTrigger create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    try {
      ValidationService.validateOrThrow(create, connection);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    if (create.name.names.size() > 1) {
      throw new DdlException(create, "Triggers cannot belong to a schema or database.");
    }
    String name = create.name.names.get(0);

    Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, true, create.target);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.target + " not found.");
    }
    SchemaPlus schemaPlus = pair.left.plus();
    Table target = schemaPlus.tables().get(pair.right);
    if (target == null) {
      throw new DdlException(create, "View/table " + create.target + " not found.");
    }

    List<String> targetSchemaPath = pair.left.path(null);
    String targetName = pair.right;
    List<String> targetPath = new ArrayList<>(targetSchemaPath);
    targetPath.add(targetName);

    Map<String, String> options = HoptimatorDdlUtils.options(create.options);

    String jobName = ((SqlLiteral) create.job).getValueAs(String.class);
    String jobNamespace = create.namespace != null
        ? ((SqlLiteral) create.namespace).getValueAs(String.class) : null;
    String cronSchedule = create.cron != null
        ? ((SqlLiteral) create.cron).getValueAs(String.class) : null;
    UserJob job = new UserJob(jobNamespace, jobName);
    Source source = new Source(databaseOf(target), targetPath, Collections.emptyMap());
    Trigger trigger = new Trigger(name, job, cronSchedule, options, source, null);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Validating trigger {} with deployers", name);
      ValidationService.validateOrThrow(trigger, connection);
      deployers = DeploymentService.deployers(trigger, connection);
      ValidationService.validateOrThrow(deployers, connection);
      logger.info("Validated trigger {}", name);
      if (create.getReplace()) {
        logger.info("Updating trigger {}", name);
        DeploymentService.update(deployers);
      } else {
        logger.info("Creating trigger {}", name);
        DeploymentService.create(deployers);
      }
      logger.info("Deployed trigger {}", name);
      logger.info("CREATE TRIGGER {} completed", name);
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw new DdlException(create, e.getMessage(), e);
    }
  }

  /**
   * Best-effort lookup of the database name that owns a Calcite Table. Mirrors what the
   * {@code DROP TABLE} path does so triggers and tables agree on the same
   * {@code (database, path)} identifier.
   */
  private static String databaseOf(Table target) {
    if (target instanceof HoptimatorJdbcTable) {
      HoptimatorJdbcSchema jdbcSchema =
          (HoptimatorJdbcSchema) ((HoptimatorJdbcTable) target).jdbcTable().jdbcSchema;
      return jdbcSchema.databaseName();
    }
    if (target instanceof TemporaryTable) {
      return ((TemporaryTable) target).databaseName();
    }
    return null;
  }

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes a {@code CREATE TABLE} command. */
  public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
    HoptimatorDdlUtils.DdlMode mode = create.getReplace()
        ? HoptimatorDdlUtils.DdlMode.UPDATE : HoptimatorDdlUtils.DdlMode.CREATE;
    try {
      HoptimatorDdlUtils.processCreateTable(context, connection, create, mode);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy table {}", create.name);
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE TABLE {} completed", create.name);
  }

  /** Executes a {@code CREATE DATABASE} command. */
  public void execute(SqlCreateDatabase create, CalcitePrepare.Context context) {
    HoptimatorDdlUtils.DdlMode mode = create.getReplace()
        ? HoptimatorDdlUtils.DdlMode.UPDATE : HoptimatorDdlUtils.DdlMode.CREATE;
    try {
      HoptimatorDdlUtils.processCreateDatabase(connection, create, mode);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy database {}", create.name);
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE DATABASE {} completed", create.name);
  }

  /** Executes a {@code PAUSE TRIGGER} command. */
  public void execute(SqlPauseTrigger pause, CalcitePrepare.Context context) {
    updateTriggerPausedState(pause, pause.name, true);
  }

  /** Executes a {@code RESUME TRIGGER} command. */
  public void execute(SqlResumeTrigger resume, CalcitePrepare.Context context) {
    updateTriggerPausedState(resume, resume.name, false);
  }

  /** Executes a {@code FIRE TRIGGER name [WITH (k v, ...)]} command.
   *  Options are merged into the trigger's job properties and the fire intent
   *  is passed to the deployer via {@link Trigger#FIRE_OPTION}; the deployer is
   *  responsible for in-flight rejection and bumping the trigger's timestamp. */
  public void execute(SqlFireTrigger fire, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", fire);
    try {
      ValidationService.validateOrThrow(fire, connection);
    } catch (SQLException e) {
      throw new DdlException(fire, e.getMessage(), e);
    }

    if (fire.name.names.size() > 1) {
      throw new DdlException(fire, "Triggers cannot belong to a schema or database.");
    }
    String name = fire.name.names.get(0);

    Map<String, String> options = HoptimatorDdlUtils.options(fire.options);
    options.put(Trigger.FIRE_OPTION, "true");
    Trigger trigger = new Trigger(name, null, null, options, null, null);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Firing trigger {} with {} option(s)", name, options.size() - 1);
      deployers = DeploymentService.deployers(trigger, connection);
      DeploymentService.update(deployers);
      logger.info("FIRE TRIGGER {} completed", name);
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw new DdlException(fire, e.getMessage(), e);
    }
  }

  /** Executes a {@code DROP TRIGGER} command. */
  public void execute(SqlDropTrigger drop, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", drop);
    try {
      ValidationService.validateOrThrow(drop, connection);
    } catch (SQLException e) {
      throw new DdlException(drop, e.getMessage(), e);
    }

    if (drop.name.names.size() > 1) {
      throw new DdlException(drop, "Triggers cannot belong to a schema or database.");
    }
    String name = drop.name.names.get(0);

    Trigger trigger = new Trigger(name, null, null, new HashMap<>(), null, null);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Deleting trigger {}", name);
      deployers = DeploymentService.deployers(trigger, connection);
      DeploymentService.delete(deployers);
      logger.info("Deleted trigger {}", name);
      logger.info("DROP TRIGGER {} completed", name);
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      // Handle IF EXISTS
      if (drop.ifExists && e.getMessage() != null && e.getMessage().contains("Error getting TableTrigger")) {
        logger.info("Trigger {} does not exist (IF EXISTS specified)", name);
        return;
      }
      throw new DdlException(drop, e.getMessage(), e);
    }
  }

  private void updateTriggerPausedState(SqlNode sqlNode, SqlIdentifier triggerName, boolean paused) {
    logger.info("Validating statement: {}", sqlNode);
    try {
      ValidationService.validateOrThrow(sqlNode, connection);
    } catch (SQLException e) {
      throw new DdlException(sqlNode, e.getMessage(), e);
    }

    if (triggerName.names.size() > 1) {
      throw new DdlException(sqlNode, "Triggers cannot belong to a schema or database.");
    }
    String name = triggerName.names.get(0);

    Map<String, String> options = new HashMap<>();
    options.put(Trigger.PAUSED_OPTION, String.valueOf(paused));
    Trigger trigger = new Trigger(name, null, null, options, null, null);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Updating trigger {} with paused state: {}", name, paused);
      deployers = DeploymentService.deployers(trigger, connection);
      DeploymentService.update(deployers);
      logger.info("Successfully updated trigger {} with paused state: {}", name, paused);
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw new DdlException(sqlNode, e.getMessage(), e);
    }
  }

  // N.B. largely copy-pasted from Apache Calcite
  /** Executes {@code DROP FUNCTION}, {@code DROP TABLE}, {@code DROP MATERIALIZED VIEW}, {@code DROP TYPE},
   * {@code DROP VIEW} commands. */
  @Override
  public void execute(SqlDropObject drop, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", drop);
    try {
      ValidationService.validateOrThrow(drop, connection);
    } catch (SQLException e) {
      throw new DdlException(drop, e.getMessage(), e);
    }

    // The logic below is only applicable for DROP VIEW and DROP MATERIALIZED VIEW.
    if (!drop.getKind().equals(SqlKind.DROP_MATERIALIZED_VIEW)
        && !drop.getKind().equals(SqlKind.DROP_VIEW)
        && !drop.getKind().equals(SqlKind.DROP_TABLE)) {
      super.execute(drop, context);
      return;
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, false, drop.name);
    String tableName = pair.right;

    SchemaPlus schemaPlus = pair.left != null ? pair.left.plus() : context.getRootSchema().plus();
    Table table = schemaPlus.tables().get(tableName);
    if (table == null) {
      if (drop.ifExists) {
        return;
      }
      throw new DdlException(drop, "Element " + tableName + " not found.");
    }

    final List<String> schemaPath = pair.left.path(null);
    List<String> tablePath = new ArrayList<>(schemaPath);
    tablePath.add(tableName);

    Collection<Deployer> deployers = null;
    try {
      if (table instanceof MaterializedViewTable) {
        if (!(drop instanceof SqlDropMaterializedView)) {
          throw new DdlException(drop,
              "Element " + tableName + " is a materialized view and does not correspond to " + drop.getOperator());
        }
        MaterializedViewTable materializedViewTable = (MaterializedViewTable) table;
        View view = new View(tablePath, materializedViewTable.viewSql());
        deployers = DeploymentService.deployers(view, connection);
        logger.info("Deleting materialized view {}", tableName);
        DeploymentService.delete(deployers);
        schemaPlus.removeTable(tableName);
        logger.info("Removed materialized table {} from schema {}", tableName, schemaPlus.getName());
      } else if (table instanceof ViewTable) {
        if (!(drop instanceof SqlDropView)) {
          throw new DdlException(drop,
              "Element " + tableName + " is a view and does not correspond to " + drop.getOperator());
        }
        ViewTable viewTable = (ViewTable) table;
        View view = new View(tablePath, viewTable.getViewSql());
        deployers = DeploymentService.deployers(view, connection);
        logger.info("Deleting view {}", tableName);
        DeploymentService.delete(deployers);
        schemaPlus.removeTable(tableName);
        logger.info("Removed view {} from schema {}", tableName, schemaPlus.getName());
      } else if (table instanceof HoptimatorJdbcTable || table instanceof TemporaryTable) {
        if (!(drop instanceof SqlDropTable)) {
          throw new DdlException(drop,
              "Element " + tableName + " is a table and does not correspond to " + drop.getOperator());
        }
        Source source;
        if (table instanceof HoptimatorJdbcTable) {
          HoptimatorJdbcTable jdbcTable = (HoptimatorJdbcTable) table;
          HoptimatorJdbcSchema jdbcSchema = (HoptimatorJdbcSchema) jdbcTable.jdbcTable().jdbcSchema;
          source = new Source(jdbcSchema.databaseName(), tablePath, Collections.emptyMap());
        } else {
          TemporaryTable temporaryTable = (TemporaryTable) table;
          source = new Source(temporaryTable.databaseName(), tablePath, Collections.emptyMap());
        }
        // Pre-delete dependency guard. PendingDelete is the explicit "delete intent" signal
        // — only validators that key off it (the K8s dep checker) fire here. The check throws
        // before any deployer-level state change.
        ValidationService.validateOrThrow(new PendingDelete<>(source), connection);
        deployers = DeploymentService.deployers(source, connection);
        logger.info("Deleting table {}", tableName);
        DeploymentService.delete(deployers);
        schemaPlus.removeTable(tableName);
        logger.info("Removed table {} from schema {}", tableName, schemaPlus.getName());
      } else {
        throw new SQLException(String.format("Unsupported drop type %s.", table.getClass().getSimpleName()));
      }
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to delete element {}", tableName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for element {}", tableName);
      }
      throw new DdlException(drop, e.getMessage(), e);
    }
    logger.info("{} completed", drop, tableName);
  }

  /** Unchecked exception related to a DDL statement. */
  static public class DdlException extends RuntimeException {

    private DdlException(SqlNode node, SqlParserPos pos, String msg, Throwable cause) {
      super("Cannot " + node.toString() + " at line " + pos.getLineNum() + " col " + pos.getColumnNum()
          + ": " + msg, cause);
    }

    DdlException(String msg, Throwable cause) {
      super(msg, cause);
    }

    DdlException(SqlNode node, String msg, Throwable cause) {
      this(node, node.getParserPosition(), msg, cause);
    }

    DdlException(SqlNode node, String msg) {
      this(node, msg, null);
    }

    DdlException(String msg) {
      this(msg, null);
    }
  }

}

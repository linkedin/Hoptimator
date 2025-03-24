/*
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
 *
 * N.B. this file copy-pasted from Apache Calcite with modifications.
 */
package com.linkedin.hoptimator.util.planner;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.linkedin.hoptimator.util.DelegatingDataSource;
import com.linkedin.hoptimator.util.DeploymentService;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class RemoteToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {

  private final Properties connectionProperties;

  protected RemoteToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      Properties connectionProperties) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    this.connectionProperties = connectionProperties;
  }

  /** This method modified from upstream */
  private SqlString generateSql(SqlDialect dialect) {
    RelRoot root = RelRoot.of(getInput(), SqlKind.SELECT);
    try {
      PipelineRel.Implementor plan = DeploymentService.plan(root, Collections.emptyList(), new Properties());
      return new SqlString(AnsiSqlDialect.DEFAULT, plan.query(connectionProperties)
          .apply(com.linkedin.hoptimator.SqlDialect.FLINK));  // TODO dialect
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RemoteToEnumerableConverter(
        getCluster(), traitSet, sole(inputs), connectionProperties);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (cost == null) {
      return null;
    }
    return cost.multiplyBy(.1);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generate:
    //   ResultSetEnumerable.of(schema.getDataSource(), "select ...")
    final BlockBuilder builder0 = new BlockBuilder(false);
    final RemoteRel child = (RemoteRel) getInput();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));
    final RemoteConvention convention =
        (RemoteConvention) requireNonNull(child.getConvention(),
            () -> "child.getConvention() is null for " + child);
    SqlString sqlString = generateSql(AnsiSqlDialect.DEFAULT); // TODO hard-coded dialect
    String sql = sqlString.getSql();
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("[" + sql + "]");
    }
    Hook.QUERY_PLAN.run(sql);
    final Expression sql_ =
        builder0.append("sql", Expressions.constant(sql));
    final int fieldCount = getRowType().getFieldCount();
    BlockBuilder builder = new BlockBuilder();
    final ParameterExpression resultSet_ =
        Expressions.parameter(Modifier.FINAL, ResultSet.class,
            builder.newName("resultSet"));
    final SqlDialect.CalendarPolicy calendarPolicy =
        AnsiSqlDialect.DEFAULT.getCalendarPolicy();  // TODO hard-coded dialect
    final Expression calendar_;
    switch (calendarPolicy) {
    case LOCAL:
      calendar_ =
          builder0.append("calendar",
              Expressions.call(Calendar.class, "getInstance",
                  getTimeZoneExpression(implementor)));
      break;
    default:
      calendar_ = null;
    }
    if (fieldCount == 1) {
      final ParameterExpression value_ =
          Expressions.parameter(Object.class, builder.newName("value"));
      builder.add(Expressions.declare(Modifier.FINAL, value_, null));
      generateGet(implementor, physType, builder, resultSet_, 0, value_,
          calendar_, calendarPolicy);
      builder.add(Expressions.return_(null, value_));
    } else {
      final Expression values_ =
          builder.append("values",
              Expressions.newArrayBounds(Object.class, 1,
                  Expressions.constant(fieldCount)));
      for (int i = 0; i < fieldCount; i++) {
        generateGet(implementor, physType, builder, resultSet_, i,
            Expressions.arrayIndex(values_, Expressions.constant(i)),
            calendar_, calendarPolicy);
      }
      builder.add(
          Expressions.return_(null, values_));
    }
    final ParameterExpression e_ =
        Expressions.parameter(SQLException.class, builder.newName("e"));
    final Expression rowBuilderFactory_ =
        builder0.append("rowBuilderFactory",
            Expressions.lambda(
                Expressions.block(
                    Expressions.return_(null,
                        Expressions.lambda(
                            Expressions.block(
                                Expressions.tryCatch(
                                    builder.toBlock(),
                                    Expressions.catch_(
                                        e_,
                                        Expressions.throw_(
                                            Expressions.new_(
                                                RuntimeException.class,
                                                e_)))))))),
                resultSet_));

    String dataSourceUrl = convention.engine().url();
    Expression dataSource = builder0.append("dataSource",
        Expressions.new_(DelegatingDataSource.class));

    builder0.add(
        Expressions.statement(
            Expressions.call(dataSource, "setUrl", Expressions.constant(dataSourceUrl))));

    final Expression enumerable;

    if (sqlString.getDynamicParameters() != null
        && !sqlString.getDynamicParameters().isEmpty()) {
      final Expression preparedStatementConsumer_ =
          builder0.append("preparedStatementConsumer",
              Expressions.call(BuiltInMethod.CREATE_ENRICHER.method,
                  Expressions.newArrayInit(Integer.class, 1,
                      toIndexesTableExpression(sqlString)),
                  DataContext.ROOT));

      enumerable =
          builder0.append("enumerable",
              Expressions.call(
                  BuiltInMethod.RESULT_SET_ENUMERABLE_OF_PREPARED.method,
                  dataSource,
                  sql_,
                  rowBuilderFactory_,
                  preparedStatementConsumer_));
    } else {
      enumerable =
          builder0.append("enumerable",
              Expressions.call(
                  BuiltInMethod.RESULT_SET_ENUMERABLE_OF.method,
                  dataSource,
                  sql_,
                  rowBuilderFactory_));
    }
    builder0.add(
        Expressions.statement(
            Expressions.call(enumerable,
                BuiltInMethod.RESULT_SET_ENUMERABLE_SET_TIMEOUT.method,
                DataContext.ROOT)));
    builder0.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, builder0.toBlock());
  }

  private static List<ConstantExpression> toIndexesTableExpression(SqlString sqlString) {
    return requireNonNull(sqlString.getDynamicParameters(),
        () -> "sqlString.getDynamicParameters() is null for " + sqlString).stream()
        .map(Expressions::constant)
        .collect(Collectors.toList());
  }

  private static UnaryExpression getTimeZoneExpression(
      EnumerableRelImplementor implementor) {
    return Expressions.convert_(
        Expressions.call(
            implementor.getRootExpression(),
            "get",
            Expressions.constant("timeZone")),
        TimeZone.class);
  }

  private static void generateGet(EnumerableRelImplementor implementor,
      PhysType physType, BlockBuilder builder, ParameterExpression resultSet_,
      int i, Expression target, @Nullable Expression calendar_,
      SqlDialect.CalendarPolicy calendarPolicy) {
    final Primitive primitive = Primitive.ofBoxOr(physType.fieldClass(i));
    final RelDataType fieldType =
        physType.getRowType().getFieldList().get(i).getType();
    final List<Expression> dateTimeArgs = new ArrayList<>();
    dateTimeArgs.add(Expressions.constant(i + 1));
    SqlTypeName sqlTypeName = fieldType.getSqlTypeName();
    boolean offset = false;
    switch (calendarPolicy) {
    case LOCAL:
      requireNonNull(calendar_, "calendar_");
      dateTimeArgs.add(calendar_);
      break;
    case NULL:
      // We don't specify a calendar at all, so we don't add an argument and
      // instead use the version of the getXXX that doesn't take a Calendar
      break;
    case DIRECT:
      sqlTypeName = SqlTypeName.ANY;
      break;
    case SHIFT:
      switch (sqlTypeName) {
      case TIMESTAMP:
      case DATE:
        offset = true;
        break;
      default:
        break;
      }
      break;
    default:
      break;
    }
    final Expression source;
    switch (sqlTypeName) {
    case DATE:
    case TIME:
    case TIMESTAMP:
      source =
          Expressions.call(
              getMethod(sqlTypeName, fieldType.isNullable(), offset),
              Expressions.<Expression>list()
                  .append(
                      Expressions.call(resultSet_,
                          getMethod2(sqlTypeName), dateTimeArgs))
                  .appendIf(offset, getTimeZoneExpression(implementor)));
      break;
    case ARRAY:
      final Expression x =
          Expressions.convert_(
              Expressions.call(resultSet_, jdbcGetMethod(primitive),
                  Expressions.constant(i + 1)),
              java.sql.Array.class);
      source = Expressions.call(BuiltInMethod.JDBC_ARRAY_TO_LIST.method, x);
      break;
    case NULL:
      source = RexImpTable.NULL_EXPR;
      break;
    default:
      source =
          Expressions.call(resultSet_, jdbcGetMethod(primitive),
              Expressions.constant(i + 1));
      break;
    }
    builder.add(
        Expressions.statement(
            Expressions.assign(
                target, source)));

    // [CALCITE-596] If primitive type columns contain null value, returns null
    // object
    if (primitive != null) {
      builder.add(
          Expressions.ifThen(
              Expressions.call(resultSet_, "wasNull"),
              Expressions.statement(
                  Expressions.assign(target,
                      Expressions.constant(null)))));
    }
  }

  private static Method getMethod(SqlTypeName sqlTypeName, boolean nullable,
      boolean offset) {
    switch (sqlTypeName) {
    case DATE:
      return (nullable
          ? (offset
          ? BuiltInMethod.DATE_TO_INT_OPTIONAL_OFFSET
          : BuiltInMethod.DATE_TO_INT_OPTIONAL)
          : (offset
              ? BuiltInMethod.DATE_TO_INT_OFFSET
              : BuiltInMethod.DATE_TO_INT)).method;
    case TIME:
      return (nullable
          ? BuiltInMethod.TIME_TO_INT_OPTIONAL
          : BuiltInMethod.TIME_TO_INT).method;
    case TIMESTAMP:
      return (nullable
          ? (offset
          ? BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL_OFFSET
          : BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL)
          : (offset
              ? BuiltInMethod.TIMESTAMP_TO_LONG_OFFSET
              : BuiltInMethod.TIMESTAMP_TO_LONG)).method;
    default:
      throw new AssertionError(sqlTypeName + ":" + nullable);
    }
  }

  private static Method getMethod2(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
    case DATE:
      return BuiltInMethod.RESULT_SET_GET_DATE2.method;
    case TIME:
      return BuiltInMethod.RESULT_SET_GET_TIME2.method;
    case TIMESTAMP:
      return BuiltInMethod.RESULT_SET_GET_TIMESTAMP2.method;
    default:
      throw new AssertionError(sqlTypeName);
    }
  }

  /** E,g, {@code jdbcGetMethod(int)} returns "getInt". */
  private static String jdbcGetMethod(@Nullable Primitive primitive) {
    return primitive == null
        ? "getObject"
        : "get" + SqlFunctions.initcap(castNonNull(primitive.primitiveName));
  }
}

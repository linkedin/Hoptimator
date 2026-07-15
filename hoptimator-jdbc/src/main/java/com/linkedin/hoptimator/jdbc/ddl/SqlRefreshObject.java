/*
 * Copy-pasted from Apache Calcite with minor modifications.
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
package com.linkedin.hoptimator.jdbc.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code REFRESH [MATERIALIZED VIEW | TABLE] <name> [FROM <bound> TO <bound>]}.
 *
 * <p>REFRESH backfills a materialized view or logical table by firing every trigger immediately
 * upstream of it. It reuses the {@code FIRE TRIGGER} machinery: an optional {@code FROM ... TO ...}
 * window requests a one-off backfill over {@code [from, to]}, exactly as for FIRE.
 *
 * <p>The {@code MATERIALIZED VIEW} / {@code TABLE} keyword is optional — the object type is
 * discoverable without it. When present, it is validated against the resolved object type, so
 * {@code REFRESH TABLE foo} on a materialized view (or vice versa) is rejected.
 */
public class SqlRefreshObject extends SqlRefresh {

  /** The object kind asserted by an optional {@code MATERIALIZED VIEW} / {@code TABLE} keyword. */
  public enum ObjectType {
    MATERIALIZED_VIEW,
    TABLE
  }

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("REFRESH", SqlKind.OTHER_DDL);

  public final SqlIdentifier name;
  /** The asserted object type, or {@code null} when the {@code MATERIALIZED VIEW}/{@code TABLE}
   *  keyword was omitted. */
  public final ObjectType objectType;
  /** Backfill window start bound, or {@code null} for a plain refresh. */
  public final SqlNode from;
  /** Backfill window end bound, or {@code null} for a plain refresh. */
  public final SqlNode to;

  public SqlRefreshObject(SqlParserPos pos, SqlIdentifier name, ObjectType objectType,
      SqlNode from, SqlNode to) {
    super(OPERATOR, pos);
    this.name = name;
    this.objectType = objectType;
    this.from = from;
    this.to = to;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, from, to);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    if (objectType == ObjectType.MATERIALIZED_VIEW) {
      writer.keyword("MATERIALIZED");
      writer.keyword("VIEW");
    } else if (objectType == ObjectType.TABLE) {
      writer.keyword("TABLE");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (from != null) {
      writer.keyword("FROM");
      from.unparse(writer, 0, 0);
      writer.keyword("TO");
      to.unparse(writer, 0, 0);
    }
  }
}

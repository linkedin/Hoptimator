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
 * Parse tree for {@code REFRESH <table> [FROM <bound> TO <bound>]}.
 *
 * <p>REFRESH backfills a physical table by firing the trigger(s) that produce it. It reuses the
 * {@code FIRE TRIGGER} machinery: an optional {@code FROM ... TO ...} window requests a one-off
 * backfill over {@code [from, to]}, exactly as for FIRE.
 *
 * <p>The target is a plain table — Hoptimator does not distinguish logical from physical, and a
 * consumer always reads a specific physical table (tier). Refreshing that table fires whatever
 * trigger writes to it.
 */
public class SqlRefreshObject extends SqlRefresh {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("REFRESH", SqlKind.OTHER_DDL);

  public final SqlIdentifier name;
  /** Backfill window start bound, or {@code null} for a plain refresh. */
  public final SqlNode from;
  /** Backfill window end bound, or {@code null} for a plain refresh. */
  public final SqlNode to;

  public SqlRefreshObject(SqlParserPos pos, SqlIdentifier name, SqlNode from, SqlNode to) {
    super(OPERATOR, pos);
    this.name = name;
    this.from = from;
    this.to = to;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, from, to);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    name.unparse(writer, leftPrec, rightPrec);
    if (from != null) {
      writer.keyword("FROM");
      from.unparse(writer, 0, 0);
      writer.keyword("TO");
      to.unparse(writer, 0, 0);
    }
  }
}

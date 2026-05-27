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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code FIRE TRIGGER} statement, optionally carrying a
 * {@code WITH (key value, ...)} options list whose pairs are merged into the
 * trigger's {@code spec.jobProperties} before the fire timestamp is bumped.
 */
public class SqlFireTrigger extends SqlFire {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("FIRE TRIGGER", SqlKind.OTHER_DDL);
  public final SqlIdentifier name;
  public final SqlNodeList options;

  public SqlFireTrigger(SqlParserPos pos, SqlIdentifier name, SqlNodeList options) {
    super(OPERATOR, pos);
    this.name = name;
    this.options = options;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, options);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("FIRE");
    writer.keyword("TRIGGER");
    name.unparse(writer, leftPrec, rightPrec);
    if (options != null) {
      writer.keyword("WITH");
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : options) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }
}

/*
 * Parts copied from Apache Calcite.
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

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
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

import static java.util.Objects.requireNonNull;


/**
 * Parse tree for {@code CREATE FUNCTION} statement.
 *
 * <pre>{@code
 * CREATE FUNCTION name [RETURNS type] AS 'class' [IN 'namespace'] [LANGUAGE lang] [WITH (...)]
 * }</pre>
 */
public class SqlCreateFunction extends SqlCreate {
  public final SqlIdentifier name;
  public final SqlNode job;
  public final SqlNode namespace;
  public final SqlDataTypeSpec returnType;
  public final SqlIdentifier language;
  public final SqlNodeList options;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE FUNCTION",
      SqlKind.CREATE_FUNCTION);

  /** Constructor with returnType and language. */
  public SqlCreateFunction(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNode job, SqlNode namespace, SqlDataTypeSpec returnType,
      SqlIdentifier language, SqlNodeList options) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = requireNonNull(name, "name");
    this.job = requireNonNull(job, "job");
    this.namespace = namespace;
    this.returnType = returnType;
    this.language = language;
    this.options = options;
  }

  /** Backward-compatible constructor without returnType and language. */
  public SqlCreateFunction(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlNode job, SqlNode namespace, SqlNodeList options) {
    this(pos, replace, ifNotExists, name, job, namespace, null, null, options);
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, job, namespace, returnType, language, options);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("FUNCTION");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (returnType != null) {
      writer.keyword("RETURNS");
      returnType.unparse(writer, 0, 0);
    }
    writer.keyword("AS");
    job.unparse(writer, 0, 0);
    if (namespace != null) {
      writer.keyword("IN");
      namespace.unparse(writer, 0, 0);
    }
    if (language != null) {
      writer.keyword("LANGUAGE");
      language.unparse(writer, 0, 0);
    }
    if (options != null) {
      writer.keyword("WITH");
      options.unparse(writer, 0, 0);
    }
  }
}

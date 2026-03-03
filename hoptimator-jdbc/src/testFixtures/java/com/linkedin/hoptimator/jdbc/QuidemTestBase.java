package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.util.DeploymentService;


public abstract class QuidemTestBase {

  protected void run(String resourceName) throws IOException, URISyntaxException {
    run(resourceName, "");
  }

  protected void run(String resourceName, String jdbcProperties) throws IOException, URISyntaxException {
    run(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(resourceName)).toURI(), jdbcProperties);
  }

  protected void run(URI resource, String jdbcProperties) throws IOException {
    File in = new File(resource);
    File out = File.createTempFile(in.getName(), ".out");
    try (Reader r = new FileReader(in, StandardCharsets.UTF_8);
         Writer w = new PrintWriter(Files.newBufferedWriter(out.toPath(), StandardCharsets.UTF_8))) {
      Quidem.Config config = Quidem.configBuilder()
          .withReader(r)
          .withWriter(w)
          .withConnectionFactory((x, y) -> DriverManager.getConnection("jdbc:hoptimator://catalogs=" + x + ";" + jdbcProperties))
          .withCommandHandler(new CustomCommandHandler())
          .build();
      new Quidem(config).execute();
    }
    List<String> input = Files.readAllLines(in.toPath(), StandardCharsets.UTF_8);
    List<String> output = Files.readAllLines(out.toPath(), StandardCharsets.UTF_8);
    Assertions.assertFalse(input.isEmpty(), "input script is empty");
    Assertions.assertFalse(output.isEmpty(), "script output is empty");
    // This check is necessary because on error, the input is echo'd to the output along with the error message
    for (String line : output) {
      System.out.println(line);
    }
    int n = Math.max(input.size(), output.size());
    for (int i = 0; i < n; i++) {
      String line = i < output.size() ? output.get(i) : "";
      String expected = i < input.size() ? input.get(i) : "";
      Assertions.assertEquals(expected, line,
        "Context:\n" + String.join("\n", output.subList(i, Math.min(i + 10, output.size() - 1))));
    }
  }

  private static final class CustomCommandHandler implements CommandHandler {
    @Override
    public Command parseCommand(List<String> lines, List<String> content, final String line) {
      List<String> copy = new ArrayList<>(lines);

      // The intention of !describe is to print the schema of a table without having to load all jdbc drivers
      // like what happens when using "metadata"."COLUMNS"
      if (line.startsWith("describe")) {
        return new AbstractCommand() {
          @Override
          public void execute(Context context, boolean execute) throws Exception {
            if (execute) {
              Connection connection = context.connection();
              if (!(connection instanceof HoptimatorConnection)) {
                throw new IllegalArgumentException("This connection doesn't support `!describe`.");
              }
              HoptimatorConnection conn = (HoptimatorConnection) connection;

              // Parse: !describe "SCHEMA"."TABLE" or !describe "CATALOG"."SCHEMA"."TABLE"
              String[] parts = line.split(" ", 2);
              if (parts.length < 2) {
                throw new IllegalArgumentException("Usage: !describe \"SCHEMA\".\"TABLE\" or \"CATALOG\".\"SCHEMA\".\"TABLE\"");
              }

              String tablePath = parts[1].trim().replaceAll("\"", "");
              String[] pathParts = tablePath.split("\\.");
              if (pathParts.length < 2) {
                throw new IllegalArgumentException("Table path must be at least SCHEMA.TABLE");
              }

              // Navigate to the schema and get the table
              SchemaPlus rootSchema = conn.calciteConnection().getRootSchema();
              SchemaPlus schema;
              String tableName;

              if (pathParts.length == 3) {
                // 3-level path: CATALOG.SCHEMA.TABLE (e.g., MySQL)
                SchemaPlus catalog = rootSchema.subSchemas().get(pathParts[0]);
                if (catalog == null) {
                  throw new IllegalArgumentException("Catalog not found: " + pathParts[0]);
                }
                schema = catalog.subSchemas().get(pathParts[1]);
                if (schema == null) {
                  throw new IllegalArgumentException("Schema not found: " + pathParts[1] + " in catalog " + pathParts[0]);
                }
                tableName = pathParts[2];
              } else {
                // 2-level path: SCHEMA.TABLE (e.g., Kafka, Venice)
                schema = rootSchema.subSchemas().get(pathParts[0]);
                if (schema == null) {
                  throw new IllegalArgumentException("Schema not found: " + pathParts[0]);
                }
                tableName = pathParts[pathParts.length - 1];
              }

              Table table = schema.tables().get(tableName);
              if (table == null) {
                throw new IllegalArgumentException("Table not found: " + tableName);
              }

              // Get the row type
              RelDataType rowType = table.getRowType(conn.calciteConnection().getTypeFactory());

              // Collect all row data first to calculate column widths
              List<String[]> rows = new ArrayList<>();
              for (RelDataTypeField field : rowType.getFieldList()) {
                String columnName = field.getName();
                SqlTypeName sqlType = field.getType().getSqlTypeName();
                String typeName = sqlType.getName();

                // Handle precision for types like VARCHAR, BINARY
                Integer precision = field.getType().getPrecision();
                String columnSize;
                if (precision != null && precision != RelDataType.PRECISION_NOT_SPECIFIED) {
                  columnSize = String.valueOf(precision);
                  // For types with precision, append it to type name
                  if (sqlType == SqlTypeName.VARCHAR || sqlType == SqlTypeName.CHAR
                      || sqlType == SqlTypeName.BINARY || sqlType == SqlTypeName.VARBINARY) {
                    if (precision > 0) {
                      typeName = typeName + "(" + precision + ")";
                    }
                  }
                } else {
                  columnSize = "null";
                }

                String isNullable = field.getType().isNullable() ? "YES" : "NO";
                rows.add(new String[]{columnName, typeName, columnSize, isNullable});
              }

              // Calculate column widths
              String[] headers = {"columnName", "typeName", "columnSize", "isNullable"};
              int[] widths = new int[headers.length];
              for (int i = 0; i < headers.length; i++) {
                widths[i] = headers[i].length();
              }
              for (String[] row : rows) {
                for (int i = 0; i < row.length; i++) {
                  widths[i] = Math.max(widths[i], row[i].length());
                }
              }

              // Build separator line
              StringBuilder separator = new StringBuilder("+");
              for (int width : widths) {
                separator.append("-".repeat(width + 2)).append("+");
              }
              String separatorLine = separator.toString();

              // Build header line
              StringBuilder headerLine = new StringBuilder("|");
              for (int i = 0; i < headers.length; i++) {
                headerLine.append(" ").append(String.format("%-" + widths[i] + "s", headers[i])).append(" |");
              }

              // Format output as a table
              List<String> output = new ArrayList<>();
              output.add(separatorLine);
              output.add(headerLine.toString());
              output.add(separatorLine);

              for (String[] row : rows) {
                StringBuilder rowLine = new StringBuilder("|");
                for (int i = 0; i < row.length; i++) {
                  rowLine.append(" ").append(String.format("%-" + widths[i] + "s", row[i])).append(" |");
                }
                output.add(rowLine.toString());
              }

              output.add(separatorLine);
              output.add("(" + rowType.getFieldCount() + " row" + (rowType.getFieldCount() == 1 ? "" : "s") + ")");
              output.add("");

              context.echo(output);
            } else {
              context.echo(content);
            }
            context.echo(copy);
          }
        };
      }

      if (line.startsWith("spec")) {
        return new AbstractCommand() {
          @Override
          public void execute(Context context, boolean execute) throws Exception {
            if (execute) {
              Connection connection = context.connection();
              if (!(connection instanceof HoptimatorConnection)) {
                throw new IllegalArgumentException("This connection doesn't support `!specify`.");
              }
              String sql = context.previousSqlCommand().sql;
              HoptimatorConnection conn = (HoptimatorConnection) connection;

              String[] parts = line.split(" ", 2);
              String viewName = parts.length == 2 ? parts[1] : "test";
              String querySql = sql;
              SqlCreateMaterializedView create = null;
              SqlNode sqlNode = HoptimatorDriver.parseQuery(conn, sql);
              if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
                if (sqlNode instanceof SqlCreateMaterializedView) {
                  create = (SqlCreateMaterializedView) sqlNode;
                  final SqlNode q = HoptimatorDdlUtils.renameColumns(create.columnList, create.query);
                  querySql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
                  viewName = HoptimatorDdlUtils.viewName(create.name);
                } else {
                  throw new RuntimeException("Unsupported DDL statement.");
                }
              }

              RelRoot root = HoptimatorDriver.convert(conn, querySql).root;
              Properties connectionProperties = conn.connectionProperties();
              RelOptTable table = root.rel.getTable();
              if (table != null) {
                connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, String.join(".", table.getQualifiedName()));
              } else if (create != null) {
                connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, create.name.toString());
              }

              PipelineRel.Implementor plan = DeploymentService.plan(root, Collections.emptyList(), connectionProperties);
              if (create != null) {
                HoptimatorDdlUtils.snapshotAndSetSinkSchema(conn.createPrepareContext(),
                    new HoptimatorDriver.Prepare(conn), plan, create, querySql);
              }
              Pipeline pipeline = plan.pipeline(viewName, conn);
              List<String> specs = new ArrayList<>();
              for (Source source : pipeline.sources()) {
                specs.addAll(DeploymentService.specify(source, conn));
              }
              specs.addAll(DeploymentService.specify(pipeline.sink(), conn));
              specs.addAll(DeploymentService.specify(pipeline.job(), conn));
              String joined = specs.stream().sorted().collect(Collectors.joining("---\n"));
              String[] lines = joined.replaceAll(";\n", "\n").split("\n");
              context.echo(Arrays.asList(lines));
            } else {
              context.echo(content);
            }
            context.echo(copy);
          }
        };
      }

      return null;
    }
  }
}

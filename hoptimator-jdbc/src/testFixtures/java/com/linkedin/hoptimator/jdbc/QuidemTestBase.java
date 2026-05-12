package com.linkedin.hoptimator.jdbc;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;

import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;
import com.linkedin.hoptimator.graph.mermaid.MermaidRenderer;
import com.linkedin.hoptimator.util.GraphService;

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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


public abstract class QuidemTestBase {

  /**
   * Extension hook — subclasses can override to register additional Quidem commands beyond
   * the built-in {@code !describe} and {@code !spec}. Return {@code null} to fall through to
   * the default handler.
   */
  protected Command parseExtraCommand(List<String> lines, List<String> content, String line) {
    return null;
  }

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

  private final class CustomCommandHandler implements CommandHandler {
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
                if (precision != RelDataType.PRECISION_NOT_SPECIFIED) {
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

      Command extra = parseExtraCommand(lines, content, line);
      if (extra != null) {
        return extra;
      }

      if (line.startsWith("graph")) {
        return new AbstractCommand() {
          @Override
          public void execute(Context context, boolean execute) throws Exception {
            if (execute) {
              Connection connection = context.connection();
              String[] parts = line.trim().split("\\s+");
              if (parts.length < 3) {
                throw new IllegalArgumentException(
                    "Usage: !graph <view|logical|table> <name|database.path> [--depth N]");
              }
              String kind = parts[1].toLowerCase();
              String identifier = parts[2];
              int depth = 2;
              for (int i = 3; i < parts.length - 1; i++) {
                if ("--depth".equals(parts[i])) {
                  depth = Integer.parseInt(parts[i + 1]);
                }
              }

              GraphTarget target = parseGraphTarget(kind, identifier);
              PipelineGraph graph = GraphService.buildGraph(target, depth, connection);
              String mermaid = GraphService.render(graph, MermaidRenderer.FORMAT);
              // Strip a trailing newline so the line list matches what's in the .id file (Quidem
              // splits the captured content on newlines without a trailing blank).
              if (mermaid.endsWith("\n")) {
                mermaid = mermaid.substring(0, mermaid.length() - 1);
              }
              // For reverse lookups, surface a Mermaid-comment warning when the graph collapses
              // to just the root node — same behavior as the sqlline `!graph` command.
              if (target instanceof GraphTarget.Resource && graph.nodes().size() == 1
                  && graph.edges().isEmpty()) {
                mermaid = mermaid + "\n%% WARNING: no pipelines reference this resource — the "
                    + "identifier may not exist or no pipelines have been deployed against it yet.";
              }
              context.echo(Arrays.asList(mermaid.split("\n")));
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

              List<String> specs = HoptimatorDdlUtils.specifyFromSql(sql, conn).specs;
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

  /**
   * Translate a Quidem-parsed {@code (kind, identifier)} into a {@link GraphTarget}. Mirrors the
   * sqlline {@code !graph} command's parser so {@code .id} scripts behave identically.
   */
  private static GraphTarget parseGraphTarget(String kind, String identifier) {
    switch (kind) {
      case "view": {
        String[] nsName = splitNamespaceName(identifier);
        return new GraphTarget.View(nsName[0], nsName[1]);
      }
      case "logical": {
        String[] nsName = splitNamespaceName(identifier);
        return new GraphTarget.LogicalTable(nsName[0], nsName[1]);
      }
      case "table": {
        String[] segments = identifier.split("\\.");
        if (segments.length < 2) {
          throw new IllegalArgumentException(
              "table identifier must be <database>.<path>; got: " + identifier);
        }
        String database = segments[0];
        List<String> path = Arrays.asList(Arrays.copyOfRange(segments, 1, segments.length));
        return new GraphTarget.Resource(database, path);
      }
      default:
        throw new IllegalArgumentException(
            "Unknown kind: " + kind + " (expected view, logical, or table)");
    }
  }

  /**
   * Splits {@code namespace/name} or returns {@code (null, name)} when no slash — the
   * provider applies its own default namespace when the caller didn't specify one.
   */
  private static String[] splitNamespaceName(String identifier) {
    int idx = identifier.indexOf('/');
    if (idx < 0) {
      return new String[] { null, identifier };
    }
    return new String[] { identifier.substring(0, idx), identifier.substring(idx + 1) };
  }
}

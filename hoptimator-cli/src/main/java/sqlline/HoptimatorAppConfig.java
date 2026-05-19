package sqlline;

import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.graph.mermaid.MermaidRenderer;
import com.linkedin.hoptimator.jdbc.GraphService;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDdlUtils;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.jdbc.ResolvedTable;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.util.Pair;
import org.jline.reader.Completer;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;


public class HoptimatorAppConfig extends Application {

  public String getInfoMessage() {
    Scanner scanner =
        new Scanner(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream("welcome.txt")),
            StandardCharsets.UTF_8);
    StringBuilder sb = new StringBuilder();
    while (scanner.hasNext()) {
      sb.append(scanner.nextLine());
      sb.append("\n");
    }
    return sb.toString();
  }

  public Collection<CommandHandler> getCommandHandlers(SqlLine sqlline) {
    Collection<CommandHandler> list = new ArrayList<>(super.getCommandHandlers(sqlline));
    list.add(new IntroCommandHandler(sqlline));
    list.add(new PipelineCommandHandler(sqlline));
    list.add(new ResolveCommandHandler(sqlline));
    list.add(new SpecifyCommandHandler(sqlline));
    list.add(new GraphCommandHandler(sqlline));
    return list;
  }

  private static final class PipelineCommandHandler implements CommandHandler {

    private final SqlLine sqlline;

    private PipelineCommandHandler(SqlLine sqlline) {
      this.sqlline = sqlline;
    }

    @Override
    public String getName() {
      return "pipeline";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Output pipeline SQL for a statement.";
    }

    @Override
    public String matches(String line) {
      if (startsWith(line, "!pipeline") || startsWith(line, "pipeline")) {
        return line;
      } else {
        return null;
      }
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      if (!(sqlline.getConnection() instanceof HoptimatorConnection)) {
        sqlline.error("This connection doesn't support `!pipeline`.");
        dispatchCallback.setToFailure();
        return;
      }
      String[] split = line.split("\\s+", 2);
      if (split.length < 2) {
        sqlline.error("Missing argument.");
        dispatchCallback.setToFailure();
        return;
      }
      String sql = split[1];
      HoptimatorConnection conn = (HoptimatorConnection) sqlline.getConnection();
      Pair<SchemaPlus, Table> schemaSnapshot = null;
      String viewName = null;
      try {
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
            sqlline.error("Unsupported DDL statement: " + sql);
            dispatchCallback.setToFailure();
            return;
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
        PipelineRel.Implementor plan = DeploymentService.plan(root, conn.materializations(), connectionProperties);
        if (create != null) {
          schemaSnapshot = HoptimatorDdlUtils.snapshotAndSetSinkSchema(conn.createPrepareContext(),
              new HoptimatorDriver.Prepare(conn), plan, create, querySql);
        }
        sqlline.output(plan.sql(conn).apply(SqlDialect.ANSI));
      } catch (SQLException e) {
        sqlline.error(e);
        dispatchCallback.setToFailure();
      }
      if (schemaSnapshot != null) {
        if (schemaSnapshot.right != null) {
          schemaSnapshot.left.add(viewName, schemaSnapshot.right);
        }
        schemaSnapshot.left.removeTable(viewName);
      }
    }

    @Override
    public List<Completer> getParameterCompleters() {
      return Collections.emptyList();
    }

    @Override
    public boolean echoToFile() {
      return false;
    }
  }

  private static final class ResolveCommandHandler implements CommandHandler {

    private final SqlLine sqlline;

    private ResolveCommandHandler(SqlLine sqlline) {
      this.sqlline = sqlline;
    }

    @Override
    public String getName() {
      return "resolve";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Resolve a table path.";
    }

    @Override
    public String matches(String line) {
      if (startsWith(line, "!resolve") || startsWith(line, "resolve")) {
        return line;
      } else {
        return null;
      }
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      if (!(sqlline.getConnection() instanceof HoptimatorConnection)) {
        sqlline.error("This connection doesn't support `!resolve`.");
        dispatchCallback.setToFailure();
        return;
      }
      String[] split = line.split("\\s+", 2);
      if (split.length < 2) {
        sqlline.error("Missing argument.");
        dispatchCallback.setToFailure();
        return;
      }
      List<String> tablePath = Arrays.asList(split[1].split("\\."));
      HoptimatorConnection conn = (HoptimatorConnection) sqlline.getConnection();
      try {
        ResolvedTable resolved = conn.resolve(tablePath, Collections.emptyMap());
        sqlline.output("Avro schema:");
        sqlline.output(resolved.avroSchemaString() + "\n");
        sqlline.output("Source configs:");
        sqlline.output(resolved.sourceConnectorConfigs().toString() + "\n");
        sqlline.output("Sink configs:");
        sqlline.output(resolved.sinkConnectorConfigs().toString() + "\n");
      } catch (SQLException e) {
        sqlline.error(e);
        dispatchCallback.setToFailure();
      }
    }

    @Override
    public List<Completer> getParameterCompleters() {
      return Collections.emptyList();
    }

    @Override
    public boolean echoToFile() {
      return false;
    }
  }


  private static final class SpecifyCommandHandler implements CommandHandler {

    private final SqlLine sqlline;

    private SpecifyCommandHandler(SqlLine sqlline) {
      this.sqlline = sqlline;
    }

    @Override
    public String getName() {
      return "specify";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Output YAML for a statement.";
    }

    @Override
    public String matches(String line) {
      if (startsWith(line, "!spec") || startsWith(line, "spec")) {
        return line;
      } else {
        return null;
      }
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      if (!(sqlline.getConnection() instanceof HoptimatorConnection)) {
        sqlline.error("This connection doesn't support `!specify`.");
        dispatchCallback.setToFailure();
        return;
      }
      String[] split = line.split("\\s+", 2);
      if (split.length < 2) {
        sqlline.error("Missing argument.");
        dispatchCallback.setToFailure();
        return;
      }
      String sql = split[1];
      HoptimatorConnection conn = (HoptimatorConnection) sqlline.getConnection();
      try {
        List<String> specs = HoptimatorDdlUtils.specifyFromSql(sql, conn).specs;
        specs.forEach(x -> sqlline.output(x + "\n\n---\n\n"));
      } catch (SQLException e) {
        sqlline.error(e);
        dispatchCallback.setToFailure();
      }
    }

    @Override
    public List<Completer> getParameterCompleters() {
      return Collections.emptyList();
    }

    @Override
    public boolean echoToFile() {
      return false;
    }
  }

  /**
   * Renders a Mermaid flowchart for a Hoptimator entity. Forms:
   * <pre>
   *   !graph &lt;schema&gt;.&lt;table&gt;                  // 2-level identifier (case-sensitive)
   *   !graph &lt;catalog&gt;.&lt;schema&gt;.&lt;table&gt;  // 3-level identifier (case-sensitive)
   * </pre>
   * Optional flag: {@code --depth N} (default 2).
   */
  static final class GraphCommandHandler implements CommandHandler {

    private final SqlLine sqlline;

    GraphCommandHandler(SqlLine sqlline) {
      this.sqlline = sqlline;
    }

    @Override
    public String getName() {
      return "graph";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Render a Mermaid pipeline graph for a view, logical table, or physical resource.";
    }

    @Override
    public String matches(String line) {
      if (startsWith(line, "!graph") || startsWith(line, "graph")) {
        return line;
      }
      return null;
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      if (!(sqlline.getConnection() instanceof HoptimatorConnection)) {
        sqlline.error("This connection doesn't support `!graph`.");
        dispatchCallback.setToFailure();
        return;
      }
      String[] parts = line.trim().split("\\s+");
      // parts[0] = "!graph" (or "graph"); identifier mandatory.
      if (parts.length < 2) {
        sqlline.error("Usage: !graph <schema.name> [--depth N]");
        dispatchCallback.setToFailure();
        return;
      }
      String identifier = parts[1];
      int depth = 2;
      for (int i = 2; i < parts.length - 1; i++) {
        if ("--depth".equals(parts[i])) {
          try {
            depth = Integer.parseInt(parts[i + 1]);
          } catch (NumberFormatException e) {
            sqlline.error("--depth requires an integer; got: " + parts[i + 1]);
            dispatchCallback.setToFailure();
            return;
          }
        }
      }

      HoptimatorConnection conn = (HoptimatorConnection) sqlline.getConnection();
      try {
        PipelineGraph graph = GraphService.buildGraph(identifier, depth, conn);
        sqlline.output(GraphService.render(graph, MermaidRenderer.FORMAT));
        // Resource targets root at an External node. A degenerate (root-only) Resource graph at
        // depth >= 1 means the label-selector found nothing — the identifier resolved to a real
        // schema, but no pipeline references it. Suppress the warning at depth <= 0 since the
        // root-only output is the depth bound's effect, not absence of pipelines.
        if (depth >= 1 && graph.root() instanceof GraphNode.External && isDegenerate(graph)) {
          sqlline.output(degenerateGraphWarning());
        }
      } catch (SQLException e) {
        sqlline.error(e);
        dispatchCallback.setToFailure();
      }
    }

    /** A graph is degenerate when it contains only the root and no edges — typical of a typo or
     * a not-yet-deployed resource. */
    static boolean isDegenerate(PipelineGraph graph) {
      return graph.nodes().size() == 1 && graph.edges().isEmpty();
    }

    /** Mermaid comment line warning that the resource may not exist. Comment syntax keeps the
     * output safe to pipe into a renderer. */
    static String degenerateGraphWarning() {
      return "%% WARNING: no pipelines reference this resource — the identifier may not exist "
          + "or no pipelines have been deployed against it yet.";
    }

    @Override
    public List<Completer> getParameterCompleters() {
      return Collections.emptyList();
    }

    @Override
    public boolean echoToFile() {
      return false;
    }
  }

  private static final class IntroCommandHandler implements CommandHandler {

    private final SqlLine sqlline;

    private IntroCommandHandler(SqlLine sqlline) {
      this.sqlline = sqlline;
    }

    @Override
    public String getName() {
      return "intro";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "What is this?";
    }

    @Override
    public String matches(String line) {
      if (startsWith(line, "!intro") || startsWith(line, "intro")) {
        return line;
      } else {
        return null;
      }
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      Scanner scanner =
          new Scanner(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream("intro.txt")),
              StandardCharsets.UTF_8);
      while (scanner.hasNext()) {
        sqlline.output(scanner.nextLine());
      }
    }

    @Override
    public List<Completer> getParameterCompleters() {
      return Collections.emptyList();
    }

    @Override
    public boolean echoToFile() {
      return false;
    }
  }

  private static boolean startsWith(String s, String prefix) {
    return s.matches("(?i)" + prefix + ".*");
  }
}

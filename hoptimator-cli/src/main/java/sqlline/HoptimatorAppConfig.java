package sqlline;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.jline.reader.Completer;

import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.PipelineRel;


public class HoptimatorAppConfig extends Application {

  public String getInfoMessage() {
    Scanner scanner =
        new Scanner(Thread.currentThread().getContextClassLoader().getResourceAsStream("welcome.txt"), "utf-8");
    StringBuilder sb = new StringBuilder();
    while (scanner.hasNext()) {
      sb.append(scanner.nextLine());
      sb.append("\n");
    }
    return sb.toString();
  }

  public Collection<CommandHandler> getCommandHandlers(SqlLine sqlline) {
    Collection<CommandHandler> list = new ArrayList<>();
    list.addAll(super.getCommandHandlers(sqlline));
    list.add(new IntroCommandHandler(sqlline));
    list.add(new PipelineCommandHandler(sqlline));
    list.add(new SpecifyCommandHandler(sqlline));
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
      try {
        CalcitePrepare.ConvertResult convert = HoptimatorDriver.convert(conn, sql);
        Map<String, Map<String, String>> tableHints = HoptimatorDriver.assembleTableHints(convert.sqlNode);
        PipelineRel.Implementor plan = DeploymentService.plan(convert.root, conn.connectionProperties(), tableHints);
        sqlline.output(plan.sql().apply(SqlDialect.ANSI));
      } catch (SQLException e) {
        sqlline.error(e);
        dispatchCallback.setToFailure();
        return;
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
      CalcitePrepare.ConvertResult convert = HoptimatorDriver.convert(conn, sql);
      Map<String, Map<String, String>> tableHints = HoptimatorDriver.assembleTableHints(convert.sqlNode);
      try {
        List<String> specs = DeploymentService.plan(convert.root, conn.connectionProperties(), tableHints)
            .pipeline("sink").specify();
        specs.forEach(x -> sqlline.output(x + "\n\n---\n\n"));
      } catch (SQLException e) {
        sqlline.error(e);
        dispatchCallback.setToFailure();
        return;
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
          new Scanner(Thread.currentThread().getContextClassLoader().getResourceAsStream("intro.txt"), "utf-8");
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

package com.linkedin.hoptimator;

import sqlline.SqlLine;
import sqlline.CommandHandler;
import sqlline.DispatchCallback;
import org.jline.reader.Completer;

import org.apache.calcite.rel.RelNode;

import com.linkedin.hoptimator.catalog.AdapterService;
import com.linkedin.hoptimator.catalog.AvroConverter;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;
import com.linkedin.hoptimator.planner.Pipeline;
import com.linkedin.hoptimator.planner.PipelineRel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.io.IOException;

public class HoptimatorCliApp {
  private final Logger logger = LoggerFactory.getLogger(HoptimatorCliApp.class);

  private SqlLine sqlline;

  public static void main(String[] args) throws Exception {
    HoptimatorCliApp app = new HoptimatorCliApp();
    int result = app.run(args);
    System.exit(result);
  }

  protected int run(String[] args) throws IOException {
    this.sqlline = new SqlLine();
    Scanner scanner = new Scanner(Thread.currentThread().getContextClassLoader().getResourceAsStream("welcome.txt"));
    while (scanner.hasNext()) {
      sqlline.output(scanner.nextLine());
    }
    List<CommandHandler> commandHandlers = new ArrayList<>();
    commandHandlers.addAll(sqlline.getCommandHandlers()); // include default handlers
    commandHandlers.add(new AvroCommandHandler());
    commandHandlers.add(new YamlCommandHandler());
    commandHandlers.add(new PipelineCommandHandler());
    commandHandlers.add(new IntroCommandHandler());
    sqlline.updateCommandHandlers(commandHandlers);
    AdapterService.adapters().forEach(x -> sqlline.output("Loaded " + x.database() + " adapter."));
    return sqlline.begin(args, null, true).ordinal();
  }
 
  private class AvroCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "avro";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Print an Avro schema derived from the given expression";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("avro")) {
        sql = sql.substring("avro".length() + 1);
        return sql;
      }

      return null;
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("avro")) {
        sql = sql.substring("avro".length() + 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        HoptimatorPlanner planner = HoptimatorPlanner.create();
        RelNode plan = planner.logical(sql);
        String avroSchema = AvroConverter.avro("OutputRecord", "OutputNamespace", plan.getRowType()).toString(true);
        sqlline.output(avroSchema); 
        dispatchCallback.setToSuccess();
      } catch (Exception e) {
        sqlline.error(e.toString());
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

  private class YamlCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "yaml";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Print YAML for a Hoptimator pipeline that implements the given query";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("yaml")) {
        sql = sql.substring("yaml".length() + 1);
        return sql;
      }

      return null;
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("yaml")) {
        sql = sql.substring("yaml".length() + 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        HoptimatorPlanner planner = HoptimatorPlanner.create();
        PipelineRel plan = planner.pipeline(sql);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        Pipeline pipeline = impl.pipeline(Collections.singletonMap("connector", "dummy"));
        // TODO provide generated avro schema to environment
        Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(new Resource.DummyEnvironment());
        sqlline.output(pipeline.render(templateFactory));
        dispatchCallback.setToSuccess();
      } catch (Exception e) {
        sqlline.error(e.toString());
        e.printStackTrace();
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

  private class PipelineCommandHandler implements CommandHandler {

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
      return "Print Hoptimator pipeline that implements the given query";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("pipeline")) {
        sql = sql.substring("pipeline".length() + 1);
        return sql;
      }

      return null;
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("pipeline")) {
        sql = sql.substring("pipeline".length() + 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        HoptimatorPlanner planner = HoptimatorPlanner.create();
        PipelineRel plan = planner.pipeline(sql);
        sqlline.output("PLAN:");
        sqlline.output(plan.explain());
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        sqlline.output("SQL:");
        sqlline.output(impl.sink(Collections.singletonMap("connector", "dummy")));
        dispatchCallback.setToSuccess();
      } catch (Exception e) {
        sqlline.error(e.toString());
        e.printStackTrace();
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

  private class IntroCommandHandler implements CommandHandler {

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
      return "What is Hoptimator?";
    }

    @Override
    public String matches(String line) {
      if (line.startsWith("!intro") || line.startsWith("intro")) {
        return line;
      } else {
        return null;
      }
    }

    @Override
    public void execute(String line, DispatchCallback dispatchCallback) {
      Scanner scanner = new Scanner(Thread.currentThread().getContextClassLoader().getResourceAsStream("intro.txt"));
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
}

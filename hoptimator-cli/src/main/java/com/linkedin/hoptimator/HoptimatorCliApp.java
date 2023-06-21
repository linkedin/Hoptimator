package com.linkedin.hoptimator;

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import sqlline.SqlLine;
import sqlline.CommandHandler;
import sqlline.DispatchCallback;
import org.jline.reader.Completer;

import org.apache.calcite.rel.RelNode;

import com.linkedin.hoptimator.catalog.AvroConverter;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;
import com.linkedin.hoptimator.planner.Pipeline;
import com.linkedin.hoptimator.planner.PipelineRel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Properties;
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
    commandHandlers.add(new InsertCommandHandler());
    commandHandlers.add(new CheckCommandHandler());
    commandHandlers.add(new MermaidCommandHandler());
    sqlline.updateCommandHandlers(commandHandlers);
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
        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        RelNode plan = planner.logical(sql);
        String avroSchema = AvroConverter.avro("OutputNamespace", "OutputName", plan.getRowType()).toString(true);
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
        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline(sql);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        HopTable outputTable = new HopTable("PIPELINE", "SINK", plan.getRowType(),
          Collections.singletonMap("connector", "dummy"));
        Pipeline pipeline = impl.pipeline(outputTable);
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
        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline(sql);
        sqlline.output("PLAN:");
        sqlline.output(plan.explain());
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        sqlline.output("SQL:");
        HopTable outputTable = new HopTable("PIPELINE", "SINK", plan.getRowType(),
          Collections.singletonMap("connector", "dummy"));
        sqlline.output(impl.insertInto(outputTable).sql(MysqlSqlDialect.DEFAULT));
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

  private class CheckCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "check";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Usage: !check <value> <query>, !check empty <query>, !check not empty <query>";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("check")) {
        sql = sql.substring("check".length() + 1);
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

      if (sql.startsWith("check")) {
        sql = sql.substring("check".length() + 1);
      }

      //remove semicolon from query if present
      if (sql.length() > 0 && sql.charAt(sql.length() - 1) == ';') {
        sql = sql.substring(0, sql.length() - 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        String[] type = sql.split(" ", 2);
        if(type.length < 2) {
          throw new IllegalArgumentException("Invalid usage"); //TODO: expand
        }

        String value = null;
        String query = null;

        String checkType=type[0];
        switch (checkType) {
          case "not":
            query = type[1].split(" ", 2)[1].trim();
            break;
          case "empty":
            query = type[1].trim();
            break;
          case "value":
            String[] valueQuery = type[1].split(" ", 2);
            value = valueQuery[0].trim();
            query = valueQuery[1].trim();
            break;
          default:
            throw new IllegalArgumentException("Expected one of 'not', 'empty', or 'value'");
        }

        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline(query);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        String pipelineSql = impl.query().sql(MysqlSqlDialect.DEFAULT);
        FlinkIterable iterable = new FlinkIterable(pipelineSql);
        Iterator<String> iter = iterable.<String>field(0, 1).iterator();
        switch(checkType) {
          case "not": 
            if (!iter.hasNext()) {
              throw new IllegalArgumentException("Expected >0 rows from query result");
            }
            break;
          case "empty":
            if (iter.hasNext()) {
              throw new IllegalArgumentException("Expected 0 rows from query result");
            }
            break;
          case "value":
            boolean varFound = false;
            while (iter.hasNext()) {
              if(String.valueOf(iter.next()).contains(value)) {
                varFound = true;
                break;
              }
            }
            if (varFound) {
              break;
            }
            throw new IllegalArgumentException("Query result did not contain expected value");
        }
        sqlline.output("PASS");
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

  private class InsertCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "insert";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Run an ephemeral pipeline with an existing sink.";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("insert into")) {
        sql = sql.substring("insert into".length() + 1);
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

      if (sql.startsWith("insert into")) {
        sql = sql.substring("insert into".length() + 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        String[] parts = sql.split("(?i)SELECT"); // case insensitive
        if (parts.length != 2) {
          throw new IllegalArgumentException("Expected ... SELECT ...");
        }
        String[] parts2 = parts[0].split("\\.");
        if (parts2.length != 2) {
          throw new IllegalArgumentException("Expected ... DATABASE.TABLE ...");
        }
        // TODO unquote correctly
        String database = parts2[0].replaceAll("[\\\"']", "").trim();
        String table = parts2[1].replaceAll("[\\\"']", "").trim();
        String query = parts[1];

        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline("SELECT " + query);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        HopTable sink = planner.database(database).makeTable(table, impl.rowType());
        String pipelineSql = impl.insertInto(sink).sql(MysqlSqlDialect.DEFAULT) + "\nSELECT 'SUCCESS';";
        FlinkIterable iterable = new FlinkIterable(pipelineSql);
        Iterator<String> iter = iterable.<String>field(0).iterator();
        if (iter.hasNext()) {
          dispatchCallback.setToSuccess();
        } else {
          throw new IllegalArgumentException("No result from:\n" + pipelineSql);
        }
        while (iter.hasNext()) {
          sqlline.output(iter.next());
        }
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

  private class MermaidCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "mermaid";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Render a pipeline in mermaid format (similar to graphviz)";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("mermaid")) {
        sql = sql.substring("mermaid".length() + 1);
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

      if (sql.startsWith("mermaid")) {
        sql = sql.substring("mermaid".length() + 1);
      }

      //remove semicolon from query if present
      if (sql.length() > 0 && sql.charAt(sql.length() - 1) == ';') {
        sql = sql.substring(0, sql.length() - 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline(sql);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        HopTable outputTable = new HopTable("PIPELINE", "SINK", plan.getRowType(),
          Collections.singletonMap("connector", "dummy"));
        Pipeline pipeline = impl.pipeline(outputTable);
        sqlline.output(pipeline.mermaid());
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
}

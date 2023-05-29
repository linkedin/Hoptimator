package com.linkedin.hoptimator;

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
import java.util.NoSuchElementException;

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
    commandHandlers.add(new CheckExpectCommandHandler());
    commandHandlers.add(new TestNonNullCommandHandler());
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
        sqlline.output(impl.insertInto(outputTable));
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
  
  private class TestNonNullCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "test";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "Verifies output of SQL query is non-empty.";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("test")) {
        sql = sql.substring("test".length() + 1);
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

      if (sql.startsWith("test")) {
        sql = sql.substring("test".length() + 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        String[] split = sql.split("(?i)SELECT", 2);
        //Should start with SELECT, so if valueSql[0].length()>0, invalid query
        if (split.length < 2 || split[0].length()>0) {
          throw new IllegalArgumentException("Expected <SQL Query>");
        }

        //Remove semicolon from query if present
        if (split[1].length() > 0 && split[1].charAt(split[1].length() - 1) == ';') {
          split[1] = split[1].substring(0, split[1].length() - 1);
        }

        String query = "SELECT " + split[1].trim();
        Integer limit = null;
        if (query.toLowerCase().contains("limit")) {
          String[] limitSplit = query.split("(?i)LIMIT");
          query = limitSplit[0].trim();
          limit = Integer.parseInt(limitSplit[1].trim());
        }

        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline(query);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        String pipelineSql = impl.query();
        FlinkIterable iterable = new FlinkIterable(pipelineSql);
        Iterator<String> iter = iterable.<String>field(0, limit).iterator();
        if (iter.hasNext()) {
          dispatchCallback.setToSuccess();
          iter.next(); //ensure next is not an error
          sqlline.output("PASS\n");
        } else {
          throw new IllegalArgumentException("No result from:\n" + pipelineSql);
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

  private class CheckExpectCommandHandler implements CommandHandler {

    @Override
    public String getName() {
      return "checkexpect";
    }

    @Override
    public List<String> getNames() {
      return Collections.singletonList(getName());
    }

    @Override
    public String getHelpText() {
      return "TODO.";
    }

    @Override
    public String matches(String line) {
      String sql = line;
      if (sql.startsWith(SqlLine.COMMAND_PREFIX)) {
        sql = sql.substring(1);
      }

      if (sql.startsWith("checkexpect")) {
        sql = sql.substring("checkexpect".length() + 1);
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

      if (sql.startsWith("checkexpect")) {
        sql = sql.substring("checkexpect".length() + 1);
      }

      String connectionUrl = sqlline.getConnectionMetadata().getUrl();
      try {
        String[] valueSql = sql.split(" ", 2);
        if (valueSql.length < 2) {
          throw new IllegalArgumentException("Expected <value> <SQL Query>");
        }

        //remove semicolon from query if present
        if (valueSql[1].length() > 0 && valueSql[1].charAt(valueSql[1].length() - 1) == ';') {
          valueSql[1] = valueSql[1].substring(0, valueSql[1].length() - 1);
        }

        String value = valueSql[0].trim();
        String query = valueSql[1];
        Integer limit = null;
        if (query.toLowerCase().contains("limit")) {
          String[] split = query.split("(?i)LIMIT");
          query = split[0];
          limit = Integer.parseInt(split[1].trim());
        }

        HoptimatorPlanner planner = HoptimatorPlanner.fromModelFile(connectionUrl, new Properties());
        PipelineRel plan = planner.pipeline(query);
        PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
        String pipelineSql = impl.query();
        FlinkIterable iterable = new FlinkIterable(pipelineSql);
        Iterator<String> iter = iterable.<String>field(0, limit).iterator();
        if (iter.hasNext()) {
          dispatchCallback.setToSuccess();
        } else {
          throw new IllegalArgumentException("No result from:\n" + pipelineSql);
        }
        while (iter.hasNext()) {
          //We define iter as Iterator<String> but it is actually generic
          //and can be an int or string. This way, we can evaluate regardless
          if(String.valueOf(iter.next()).contains(value)) {
            sqlline.output("PASS");
            return;
          }
        }
        throw new NoSuchElementException("No element " + value + " found.");
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
      return "Todo";
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
        String pipelineSql = impl.insertInto(sink) + "\nSELECT 'SUCCESS';";
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
        String pipelineSql = impl.insertInto(sink) + "\nSELECT 'SUCCESS';";
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
}

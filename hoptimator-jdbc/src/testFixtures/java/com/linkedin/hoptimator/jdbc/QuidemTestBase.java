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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
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

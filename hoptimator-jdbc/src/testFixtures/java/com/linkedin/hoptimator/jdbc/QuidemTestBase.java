package com.linkedin.hoptimator.jdbc;

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

import org.apache.calcite.rel.RelRoot;
import org.junit.jupiter.api.Assertions;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import com.linkedin.hoptimator.util.DeploymentService;


public abstract class QuidemTestBase {

  protected void run(String resourceName) throws IOException, URISyntaxException {
    run(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(resourceName)).toURI(), "");
  }

  protected void run(String resourceName, String jdbcProperties) throws IOException, URISyntaxException {
    run(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(resourceName)).toURI(), jdbcProperties);
  }

  protected void run(URI resource, String jdbcProperties) throws IOException {
    File in = new File(resource);
    File out = File.createTempFile(in.getName(), ".out");
    try (Reader r = new FileReader(in); Writer w = new PrintWriter(out)) {
      Quidem.Config config = Quidem.configBuilder()
          .withReader(r)
          .withWriter(w)
          .withConnectionFactory((x, y) -> DriverManager.getConnection("jdbc:hoptimator://catalogs=" + x + jdbcProperties))
          .withCommandHandler(new CustomCommandHandler())
          .build();
      new Quidem(config).execute();
    }
    List<String> input = Files.readAllLines(in.toPath(), StandardCharsets.UTF_8);
    List<String> output = Files.readAllLines(out.toPath(), StandardCharsets.UTF_8);
    Assertions.assertFalse(input.isEmpty(), "input script is empty");
    Assertions.assertFalse(output.isEmpty(), "script output is empty");
    for (String line : output) {
      System.out.println(line);
    }
    Assertions.assertIterableEquals(input, output);
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
              try (Connection connection = context.connection()) {
                if (!(connection instanceof HoptimatorConnection)) {
                  throw new IllegalArgumentException("This connection doesn't support `!specify`.");
                }
                String sql = context.previousSqlCommand().sql;
                HoptimatorConnection conn = (HoptimatorConnection) connection;
                RelRoot root = HoptimatorDriver.convert(conn, sql).root;
                String []parts = line.split(" ", 2);
                String pipelineName = parts.length == 2 ? parts[1] : "test";
                String specs =
                    DeploymentService.plan(root, conn.connectionProperties())
                        .pipeline(pipelineName).specify().stream().sorted()
                        .collect(Collectors.joining("---\n"));
                String[] lines = specs.replaceAll(";\n", "\n").split("\n");
                context.echo(Arrays.asList(lines));
              }
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

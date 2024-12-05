package com.linkedin.hoptimator.jdbc;

import net.hydromatic.quidem.Quidem;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;

public abstract class QuidemTestBase {

  protected void run(String resourceName) throws IOException, URISyntaxException {
    run(Thread.currentThread().getContextClassLoader().getResource(resourceName).toURI());
  }

  protected void run(URI resource) throws IOException {
    File in = new File(resource);
    File out = File.createTempFile(in.getName(), ".out");
    try (Reader r = new FileReader(in);
        Writer w = new PrintWriter(out)) {
      Quidem.Config config = Quidem.configBuilder()
          .withReader(r)
          .withWriter(w)
          .withConnectionFactory((x, y) -> DriverManager.getConnection("jdbc:hoptimator://" + x))
          .build();
      new Quidem(config).execute(); 
    }
    List<String> input = Files.readAllLines(in.toPath(), StandardCharsets.UTF_8);
    List<String> output = Files.readAllLines(out.toPath(), StandardCharsets.UTF_8);
    Assertions.assertTrue(!input.isEmpty(), "input script is empty");
    Assertions.assertTrue(!output.isEmpty(), "script output is empty");
    System.out.println(out.toPath().toString());
    for(String line : output) {
      System.out.println(line);
    }
    Assertions.assertIterableEquals(input, output);
  }
}

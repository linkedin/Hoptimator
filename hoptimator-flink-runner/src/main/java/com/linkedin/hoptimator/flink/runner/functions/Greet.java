package com.linkedin.hoptimator.flink.runner.functions;

import org.apache.flink.table.functions.ScalarFunction;


/** A simple scalar UDF that greets a name. */
public class Greet extends ScalarFunction {

  public String eval(String name) {
    if (name == null) {
      return null;
    }
    return "Hello, " + name + "!";
  }
}

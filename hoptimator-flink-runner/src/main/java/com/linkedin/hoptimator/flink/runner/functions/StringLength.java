package com.linkedin.hoptimator.flink.runner.functions;

import org.apache.flink.table.functions.ScalarFunction;


/** A simple scalar UDF that returns the length of a string. */
public class StringLength extends ScalarFunction {

  public Integer eval(String s) {
    if (s == null) {
      return null;
    }
    return s.length();
  }
}

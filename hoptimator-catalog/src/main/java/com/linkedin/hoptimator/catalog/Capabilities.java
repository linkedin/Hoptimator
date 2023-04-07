package com.linkedin.hoptimator.catalog;

/** Experimental Capabilities API */
public interface Capabilities {

  float IMPOSSIBLE = Float.MAX_VALUE;
  float EXPENSIVE = 100.0f;
  float CHEAP = 1.0f;
  float FREE = 0.0f;

  /** Cost to read from the tail of the table, i.e. to read recently written data. */
  default float tailReadCost() {
    return IMPOSSIBLE;
  }

  /** Cost to read from the head of the table, i.e. the least-recently written data. *
  default float headReadCost() {
    return IMPOSSIBLE;
  }

  /** Cost to read from a random location in the table. */
  default float randomReadCost() {
    return IMPOSSIBLE;
  }

  /** Cost to write to the table */
  default float randomWriteCost() {
    return IMPOSSIBLE;
  }

  /** e.g. Kafka */
  interface AppendOnlyLog extends Capabilities {

    default float tailReadCost() {
      return CHEAP;
    }

    default float tailWriteCost() {
      return CHEAP;
    }
  }

  /** e.g. Espresso */
  interface KeyValueStore extends Capabilities {

    default float tailReadCost() {
      return EXPENSIVE;
    }

    default float tailWriteCost() {
      return EXPENSIVE;
    } 

    default float randomReadCost() {
      return EXPENSIVE;
    } 

    default float randomWriteCost() {
      return EXPENSIVE;
    } 
  }
}

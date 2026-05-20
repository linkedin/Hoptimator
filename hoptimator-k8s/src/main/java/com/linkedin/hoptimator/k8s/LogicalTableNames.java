package com.linkedin.hoptimator.k8s;


/**
 * Canonical names for the K8s objects implicitly created by the LogicalTable deployer
 * (inter-tier {@code Pipeline} CRDs and the offline-tier {@code TableTrigger} CRD).
 *
 * <p>Shared between the producer (the deployer in {@code hoptimator-logical}) and consumers
 * (the visualizer's {@code PipelineGraphBuilder} in {@code hoptimator-k8s}) so name-based
 * deduction stays in lockstep with creation. If the producer ever changes the scheme, every
 * consumer that derives names breaks at compile time when this class moves.
 *
 * <p>Inputs use the un-prefixed table name (e.g. {@code "testevent"}, not
 * {@code "logical-testevent"}) — i.e. {@code LogicalTable.spec.tableName}.
 */
public final class LogicalTableNames {

  private static final String TRIGGER_NAME_FORMAT = "logical-%s-offline-trigger";

  private LogicalTableNames() {
  }

  /** CRD name for the implicit inter-tier Pipeline between {@code fromTier} and {@code toTier}. */
  public static String pipelineName(String tableName, String fromTier, String toTier) {
    return "logical-" + K8sUtils.canonicalizeName(tableName) + "-" + fromTier + "-to-" + toTier;
  }

  /** CRD name for the offline-tier TableTrigger; only meaningful when an offline tier is present. */
  public static String triggerName(String tableName) {
    return String.format(TRIGGER_NAME_FORMAT, K8sUtils.canonicalizeName(tableName));
  }
}

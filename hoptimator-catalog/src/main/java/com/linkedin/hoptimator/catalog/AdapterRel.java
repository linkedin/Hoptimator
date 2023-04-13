package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Calling convention that ultimately gets converted to a Pipeline or similar.
 *
 * For now, Hoptimator only implements Pipelines (via PipelineRel), which run on
 * Flink. Eventually, PipelineRel may support additional runtimes (e.g. Spark),
 * and/or Hoptimator may support additional calling conventions (e.g. batch jobs
 * or build-and-push jobs). This calling convention -- and indeed, this entire
 * module -- makes no assumptions about the compute layer.
 *  
 */
public interface AdapterRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ADAPTER", AdapterRel.class);
}

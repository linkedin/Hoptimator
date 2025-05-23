package com.linkedin.hoptimator;

import java.util.Set;


/**
 * A job, along with its sources and sink.
 */
public class Pipeline {

  private final Set<Source> sources;
  private final Sink sink;
  private final Job job;

  public Pipeline(Set<Source> sources, Sink sink, Job job) {
    this.sources = sources;
    this.sink = sink;
    this.job = job;
  }

  public Set<Source> sources() {
    return sources;
  }

  public Sink sink() {
    return sink;
  }

  public Job job() {
    return job;
  }
}

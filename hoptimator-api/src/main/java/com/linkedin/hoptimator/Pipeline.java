package com.linkedin.hoptimator;

import java.util.Collection;


/**
 * A job, along with its sources and sink.
 */
public class Pipeline {

  private final Collection<Source> sources;
  private final Sink sink;
  private final Job job;

  public Pipeline(Collection<Source> sources, Sink sink, Job job) {
    this.sources = sources;
    this.sink = sink;
    this.job = job;
  }

  public Collection<Source> sources() {
    return sources;
  }

  public Sink sink() {
    return sink;
  }

  public Job job() {
    return job;
  }
}

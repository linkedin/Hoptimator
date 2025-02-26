package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Collection;


/**
 * A job, along with its sources and sink.
 */
public class Pipeline {

  private Collection<Source> sources;
  private Sink sink;
  private Job job;

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

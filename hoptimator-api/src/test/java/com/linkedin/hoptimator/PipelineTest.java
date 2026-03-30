package com.linkedin.hoptimator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class PipelineTest {

  @Test
  void testAccessors() {
    Source source = new Source("db", List.of("src"), Collections.emptyMap());
    Sink sink = new Sink("db", List.of("sink"), Collections.emptyMap());
    Job job = new Job("j", Collections.emptySet(), sink, Collections.emptyMap());
    List<Source> sources = List.of(source);

    Pipeline pipeline = new Pipeline(sources, sink, job);

    assertEquals(sources, List.copyOf(pipeline.sources()));
    assertEquals(sink, pipeline.sink());
    assertEquals(job, pipeline.job());
    assertNotNull(pipeline.sources());
  }
}

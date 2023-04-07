package com.linkedin.hoptimator.planner;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Collections;

public final class HoptimatorPlannerDemo {

  private HoptimatorPlannerDemo() {
  }

  public static void main(String[] args) throws Exception {

    String sql = "SELECT c.NAME, p.AGE FROM DATAGEN.PERSON p, DATAGEN.COMPANY c WHERE p.NAME=c.CEO";
    HoptimatorPlanner planner = HoptimatorPlanner.create();
    PipelineRel plan = planner.pipeline(sql);
    PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);
    Pipeline pipeline = impl.pipeline(Collections.singletonMap("connector", "dummy"));
    Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(new Resource.DummyEnvironment());
    String yaml = pipeline.render(templateFactory);
    System.out.println(yaml);
  }
}

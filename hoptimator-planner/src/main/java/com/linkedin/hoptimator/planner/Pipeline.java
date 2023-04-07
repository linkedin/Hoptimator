package com.linkedin.hoptimator.planner;

import org.apache.calcite.rel.type.RelDataType;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Set;

/** A set of Resources that deliver data.
 *
 */
public class Pipeline {
  private final Set<Resource> resources;
  private final RelDataType outputType;

  public Pipeline(Set<Resource> resources, RelDataType outputType) {
    this.resources = resources;
    this.outputType = outputType;
  }

  public RelDataType outputType() {
    return outputType;
  }

  public Set<Resource> resources() {
    return resources;
  }

  /** Render all resources as one big YAML stream */
  public String render(Resource.TemplateFactory templateFactory) {
    StringBuilder sb = new StringBuilder();
    for (Resource resource : resources) {
      sb.append(templateFactory.get(resource).render(resource));
      sb.append("\n---\n"); // yaml resource separator
    }
    return sb.toString();
  }
}

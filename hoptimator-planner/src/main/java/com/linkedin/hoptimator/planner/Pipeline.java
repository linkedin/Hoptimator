package com.linkedin.hoptimator.planner;

import org.apache.calcite.rel.type.RelDataType;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A set of Resources that deliver data.
 *
 */
public class Pipeline {
  private final Collection<Resource> resources;
  private final RelDataType outputType;

  public Pipeline(Collection<Resource> resources, RelDataType outputType) {
    this.resources = resources;
    this.outputType = outputType;
  }

  public RelDataType outputType() {
    return outputType;
  }

  public Collection<Resource> resources() {
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

  /** Render a graph of resources in mermaid format */
  public String mermaid() {
    StringBuilder sb = new StringBuilder();
    sb.append("flowchart\n");
    Map<String, List<Resource>> grouped = resources.stream()
      .collect(Collectors.groupingBy(x -> x.kind()));
    grouped.forEach((k, v) -> {
      sb.append("  subgraph " + k + "\n");
      v.forEach(x -> {
        String description = x.keys().stream()
          .filter(k2 -> x.property(k2) != null)
          .filter(k2 -> !x.property(k2).isEmpty())
          .filter(k2 -> !"id".equals(k2))
          .map(k2 -> k2 + ": " + sanitize(x.property(k2)))
          .collect(Collectors.joining("\n"));
        sb.append("  " + id(x) + "[\"" + description + "\"]\n");
      });
      sb.append("  end\n");
    });
    grouped.forEach((k, v) -> {
      sb.append("  subgraph " + k + "\n");
      v.forEach(x -> {
        x.inputs().forEach(y -> {
          sb.append("    " + id(y) + " --> " + id(x) + "\n");
        });
      });
      sb.append("  end\n");
    });
    return sb.toString();
  }

  private static String id(Resource resource) {
    return "R" + Integer.toString(resource.hashCode());
  }

  private static String sanitize(String s) {
    String safe = s.replaceAll("\"", "&quot;").replaceAll("\n", " ").trim();
    if (safe.length() > 20) {
      return safe.substring(0, 17) + "..."; 
    }
    return safe;
  }
}

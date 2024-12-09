package com.linkedin.hoptimator.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.ResourceProvider;


/** A set of Resources that deliver data.
 *
 */
public class Pipeline {
  private final Collection<Resource> upstreamResources;
  private final Collection<Resource> downstreamResources;
  private final SqlJob sqlJob;
  private final RelDataType outputType;

  public Pipeline(Collection<Resource> upstreamResources, SqlJob sqlJob, Collection<Resource> downstreamResources,
      RelDataType outputType) {
    this.upstreamResources = upstreamResources;
    this.sqlJob = sqlJob;
    this.downstreamResources = downstreamResources;
    this.outputType = outputType;
  }

  public RelDataType outputType() {
    return outputType;
  }

  /** The SQL job which writes to the sink. */
  public SqlJob sqlJob() {
    return sqlJob;
  }

  /** Resources upstream of the SQL job, corresponding to all sources. */
  public Collection<Resource> upstreamResources() {
    return upstreamResources;
  }

  /** Resources downstream of the SQL job, corresponding to the sink. */
  public Collection<Resource> downstreamResources() {
    return downstreamResources;
  }

  /** All Resources in the pipeline, including the SQL job and sink. */
  public Collection<Resource> resources() {
    // We re-use ResourceProvider here for its source->sink relationships
    ResourceProvider resourceProvider =
        ResourceProvider.from(upstreamResources).to(sqlJob).toAll(x -> downstreamResources);

    // All resources are now "provided", so we can pass null here:
    return resourceProvider.resources(null);
  }

  /** Render all resources */
  public Collection<String> render(Resource.TemplateFactory templateFactory) throws IOException {
    List<String> res = new ArrayList<>();
    for (Resource resource : resources()) {
      res.addAll(templateFactory.render(resource));
    }
    return res;
  }

  /** Render a graph of resources in mermaid format */
  public String mermaid() {
    StringBuilder sb = new StringBuilder();
    sb.append("flowchart\n");
    Map<String, List<Resource>> grouped = resources().stream().collect(Collectors.groupingBy(x -> x.template()));
    grouped.forEach((k, v) -> {
      sb.append("  subgraph " + k + "\n");
      v.forEach(x -> {
        String description = x.keys()
            .stream()
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

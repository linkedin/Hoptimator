package com.linkedin.hoptimator.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enables an adapter to emit arbitrary Resources for a given table.
 *
 * Optionally, establishes source->sink relationships between such Resources. These are used
 * strictly for debugging purposes.
 */
public interface ResourceProvider {

  /** Resources for the given table */
  Collection<Resource> resources(String tableName);

  /**
   * Establishes a source->sink relationship between this ResourceProvider and a sink Resource.
   *
   * All leaf-node Resources provided by this ResourceProvider will become inputs to the sink.
   *
   * e.g.
   * <pre>
   *   ResourceProvider.empty().with(x -> a).with(x -> b).to(x -> c).to(x -> d)
   * </pre>
   *
   * encodes the following DAG:
   * <pre>
   *   a --> c
   *   b --> c
   *   c --> d
   * </pre>
   */
  default ResourceProvider toAll(ResourceProvider sink) {
    return x -> {
      List<Resource> combined = new ArrayList<>();
      List<Resource> sources = new ArrayList<>();
      List<Resource> sinks = new ArrayList<>();
      sources.addAll(resources(x));
      combined.addAll(sources);

      // remove all non-leaf-node upstream Resources
      sources.removeAll(sources.stream().flatMap(y -> y.inputs().stream())
        .collect(Collectors.toList()));

      // link all sources to all sinks       
      sink.resources(x).forEach(y -> {
        combined.add(new Resource(y) {{
          sources.forEach(z -> input(z));
        }});
      });

      return combined;
    };
  }

  default ResourceProvider to(Resource resource) {
    return toAll(x -> Collections.singleton(resource));
  }

  default ResourceProvider to(Function<String, Resource> resourceFunc) {
    return toAll(x -> Collections.singleton(resourceFunc.apply(x)));
  }

  /** Combines this ResourceProvider with another ResourceProvider */
  default ResourceProvider withAll(ResourceProvider resourceProvider) {
    return x -> {
      List<Resource> combined = new ArrayList<>();
      combined.addAll(resources(x));
      combined.addAll(resourceProvider.resources(x));
      return combined;
    };
  }

  default ResourceProvider with(Resource resource) {
    return withAll(x -> Collections.singleton(resource));
  }

  default ResourceProvider with(Function<String, Resource> resourceFunc) {
    return withAll(x -> Collections.singleton(resourceFunc.apply(x)));
  }

  static ResourceProvider empty() {
    return x -> Collections.emptyList();
  }

  static ResourceProvider from(Collection<Resource> resources) {
    return x -> resources;
  }

  static ResourceProvider from(Resource resource) {
    return x -> Collections.singleton(resource);
  }
}

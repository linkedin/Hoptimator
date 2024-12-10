package com.linkedin.hoptimator.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Enables an adapter to emit arbitrary Resources for a given table.
 * <p>
 * Optionally, establishes source->sink relationships between such Resources. These are used
 * strictly for debugging purposes.
 */
public interface ResourceProvider {

  /** Resources for the given table */
  Collection<Resource> resources(String tableName);

  /** Resources required when reading from the table */
  default Collection<Resource> readResources(String tableName) {
    return resources(tableName).stream().filter(x -> !(x instanceof WriteResource)).collect(Collectors.toList());
  }

  /** Resources required when writing to the table */
  default Collection<Resource> writeResources(String tableName) {
    return resources(tableName).stream().filter(x -> !(x instanceof ReadResource)).collect(Collectors.toList());
  }

  /**
   * Establishes a source->sink relationship between ResourceProviders.
   * <p>
   * All leaf-node Resources provided by this ResourceProvider will become sources. All nodes
   * provided by the given ResourceProvider will be sinks.
   * <p>
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
      List<Resource> sinks = new ArrayList<>();
      List<Resource> sources = new ArrayList<>(resources(x));
      List<Resource> combined = new ArrayList<>(sources);

      // remove all non-leaf-node upstream Resources
      sources.removeAll(sources.stream().flatMap(y -> y.inputs().stream()).collect(Collectors.toList()));

      // remove all read/write-only upstream Resources
      sources.removeAll(sources.stream()
          .filter(y -> y instanceof ReadResource || y instanceof WriteResource)
          .collect(Collectors.toList()));

      // link all sources to all sinks
      sink.resources(x).forEach(y -> combined.add(new Resource(y) {{
        if (!(y instanceof ReadResource || y instanceof WriteResource)) {
          sources.forEach(this::input);
        }
      }}));

      return combined;
    };
  }

  /** Provide a sink resource. */
  default ResourceProvider to(Resource resource) {
    return toAll(x -> Collections.singleton(resource));
  }

  /** Provide a sink resource. */
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

  /** Provide a resource. */
  default ResourceProvider with(Resource resource) {
    return withAll(x -> Collections.singleton(resource));
  }

  /** Provide a resource. */
  default ResourceProvider with(Function<String, Resource> resourceFunc) {
    return withAll(x -> Collections.singleton(resourceFunc.apply(x)));
  }

  /** Provide the given resources, but only when the table needs to be read from. */
  default ResourceProvider readWithAll(ResourceProvider readResourceProvider) {
    return x -> {
      List<Resource> combined = new ArrayList<>();
      combined.addAll(resources(x));
      combined.addAll(
          readResourceProvider.resources(x).stream().map(ReadResource::new).collect(Collectors.toList()));
      return combined;
    };
  }

  /** Provide the given resources, but only when the table needs to be written to. */
  default ResourceProvider writeWithAll(ResourceProvider writeResourceProvider) {
    return x -> {
      List<Resource> combined = new ArrayList<>();
      combined.addAll(resources(x));
      combined.addAll(
          writeResourceProvider.resources(x).stream().map(WriteResource::new).collect(Collectors.toList()));
      return combined;
    };
  }

  /** Provide the given resource, but only when the table needs to be read from. */
  default ResourceProvider readWith(Function<String, Resource> resourceFunc) {
    return readWithAll(x -> Collections.singleton(resourceFunc.apply(x)));
  }

  /** Provide the given resource, but only when the table needs to be read from. */
  default ResourceProvider readWith(Resource resource) {
    return readWithAll(x -> Collections.singleton(resource));
  }

  /** Provide the given resource, but only when the table needs to be written to. */
  default ResourceProvider writeWith(Function<String, Resource> resourceFunc) {
    return writeWithAll(x -> Collections.singleton(resourceFunc.apply(x)));
  }

  /** Provide the given resource, but only when the table needs to be written to. */
  default ResourceProvider writeWith(Resource resource) {
    return writeWithAll(x -> Collections.singleton(resource));
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

  /** A Resource that shouldn't be provided when reading from the table. */
  class ReadResource extends Resource {

    public ReadResource(Resource resource) {
      super(resource);
    }
  }

  /** A Resource that shouldn't be provided when wiring to the table. */
  class WriteResource extends Resource {

    public WriteResource(Resource resource) {
      super(resource);
    }
  }
}

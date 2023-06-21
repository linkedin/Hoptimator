package com.linkedin.hoptimator.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Scanner;
import java.util.function.Supplier;
import java.io.InputStream;
import java.util.stream.Collectors;

/**
 * Represents something required by a Table.
 *
 * In Hoptimator, Tables can come with "baggage" in the form of Resources,
 * which are essentially YAML files. Each Resource is rendered into YAML
 * with a specific Template. Thus, it's possible to represent Kafka topics,
 * Brooklin datastreams, Flink jobs, and so on, as part of a Table, just
 * by including a corresponding Resource.
 *
 * Resources are injected into a Pipeline by the planner as needed. Generally,
 * each Resource Template corresponds to a Kubernetes controller.
 *
 * Resources may optionally link to input Resources, which is used strictly
 * for informational/debugging purposes.
 */
public abstract class Resource {
  private final String kind;
  private final SortedMap<String, Supplier<String>> properties = new TreeMap<>();
  private final List<Resource> inputs = new ArrayList<>();

  /** A Resource of some kind. */
  public Resource(String kind) {
    this.kind = kind;
  }

  /** Copy constructor */
  public Resource(Resource other) {
    this.kind = other.kind;
    this.properties.putAll(other.properties);
    this.inputs.addAll(other.inputs);
  }

  public String kind() {
    return kind;
  }

  /** Export a computed value to the template */
  protected void export(String key, Supplier<String> supplier) {
    properties.put(key, supplier);
  }

  /** Export a static value to the template */
  protected void export(String key, String value) {
    properties.put(key, () -> value);
  }

  /** Export a map of values */
  protected void export(String key, Map<String, String> values) {
    export(key, values.entrySet().stream().map(x -> x.getKey() + ": " + x.getValue())
      .collect(Collectors.joining("\n")));
  }

  /** Export a list of values */
  protected void export(String key, List<String> values) {
    export(key, values.stream().collect(Collectors.joining("\n")));
  }

  /** Reference an input resource */
  protected void input(Resource resource) {
    inputs.add(resource);
  }

  public String property(String key) {
    return getOrDefault(key, null);
  }

  /** Keys for all defined properties, in natural order. */
  public Set<String> keys() {
    return properties.keySet();
  }

  /** Render this Resource using the given TemplateFactory */
  public String render(TemplateFactory templateFactory) {
    return templateFactory.get(this).render(this);
  }

  public String render(Template template) {
    return template.render(this);
  }

  public Collection<Resource> inputs() {
    return inputs;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  } 

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ kind: " + kind() + " ");
    for (Map.Entry<String, Supplier<String>> entry : properties.entrySet()) {
      String value = entry.getValue().get();
      if (value != null && !value.isEmpty()) {
        sb.append(entry.getKey());
        sb.append(":");
        sb.append(entry.getValue().get());
        sb.append(" ");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  public String getOrDefault(String key, Supplier<String> f) {
    String computed = properties.getOrDefault(key, f).get();
    if (computed == null) {
      throw new IllegalArgumentException("Resource missing variable '" + key + "'");
    }
    return computed;
  }

  /** Exposes environment variables to templates */
  public interface Environment {
    Environment EMPTY = new SimpleEnvironment();
    Environment PROCESS = new ProcessEnvironment();

    String get(String key);
  }

  /** Basic Environment implementation */
  public static class SimpleEnvironment implements Environment {
    private final Map<String, String> vars;

    public SimpleEnvironment(Map<String, String> vars) {
      this.vars = vars;
    }

    public SimpleEnvironment() {
      this(new HashMap<>());
    }

    public void export(String property, String value) {
      vars.put(property, value);
    }

    public SimpleEnvironment(Properties properties) {
      this.vars = new HashMap<>();
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        this.vars.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    public SimpleEnvironment with(String key, String value) {
      Map<String, String> newVars = new HashMap<>();
      newVars.putAll(vars);
      newVars.put(key, value);
      return new SimpleEnvironment(newVars);
    }

    @Override
    public String get(String key) {
      if (!vars.containsKey(key)) {
        throw new IllegalArgumentException("No variable '" + key + "' found in the environment");
      }
      return vars.get(key);
    }
  }

  /** Returns "{{key}}" for any key */
  public static class DummyEnvironment implements Environment {
    @Override
    public String get(String key) {
      return "{{" + key + "}}";
    }
  }

  /** Provides access to the process's environment variables */
  public static class ProcessEnvironment implements Environment {

    @Override
    public String get(String key) {
      String value = System.getenv(key);
      if (value == null) {
        value = System.getProperty(key);
      }
      if (value == null) {
        throw new IllegalArgumentException("Missing system property `" + key + "`");
      }
      return value;
    }
  }

  /** Turns a Resource into a String. Intended for generating K8s YAML. */
  public interface Template {
    String render(Resource resource);
  }

  /**
   * Replaces `{{var}}` in a template file with the corresponding variable.
   *
   * Resource-scoped variables take precedence over Environment-scoped variables.
   *
   * If `var` contains multiple lines, the behavior depends on context; specifically,
   * whether the pattern appears within a list or comment (prefixed with `-` or `#`).
   * For example, if the template includes:
   *
   *   - {{var}}
   *
   * ...and `var` contains multiple lines, then the output will be:
   *
   *   - value line 1
   *   - value line 2
   *
   * To avoid this behavior (and just get a multiline string), use one of YAML's multiline
   * markers, e.g.
   *
   *   - |
   *       {{var}}
   *
   * In either case, the multiline string will be properly indented.
   */
  public static class SimpleTemplate implements Template {
    private final Environment env;
    private final String template;

    public SimpleTemplate(Environment env, String template) {
      this.env = env;
      this.template = template;
    }
        
    @Override
    public String render(Resource resource) {
      StringBuffer sb = new StringBuffer();
      Pattern p = Pattern.compile("([\\s\\-\\#]*)\\{\\{\\s*([\\w_\\-\\.]+)\\s*\\}\\}");
      Matcher m = p.matcher(template);
      while (m.find()) {
        String prefix = m.group(1);
        if (prefix == null) {
          prefix = "";
        }
        String key = m.group(2);
        String value = resource.getOrDefault(key, () -> env.get(key));
        if (value == null) {
          throw new IllegalArgumentException("No value for key " + key);
        }
        String quotedPrefix = Matcher.quoteReplacement(prefix);
        String quotedValue = Matcher.quoteReplacement(value);
        String replacement = quotedPrefix + quotedValue.replaceAll("\\n", quotedPrefix);
        m.appendReplacement(sb, replacement);
      }
      m.appendTail(sb); 
      return sb.toString();
    }
  }

  /** Locates a Template for a given Resource */
  public interface TemplateFactory {
    Template get(Resource resource);
  }
 
  /** Finds a Template for a given Resource by looking for resource files in the classpath. */ 
  public static class SimpleTemplateFactory implements TemplateFactory {
    private final Environment env;

    public SimpleTemplateFactory(Environment env) {
      this.env = env;
    }

    @Override
    public Template get(Resource resource) {
      String kind = resource.kind();
      InputStream in = getClass().getClassLoader().getResourceAsStream(kind + ".yaml.template");
      if (in == null) {
        throw new IllegalArgumentException("No template '" + kind + "' found in jar resources");
      }
      StringBuilder sb = new StringBuilder();
      Scanner scanner = new Scanner(in);
      scanner.useDelimiter("\n");
      while (scanner.hasNext()) {
        sb.append(scanner.next());
        sb.append("\n");
      }
      return new SimpleTemplate(env, sb.toString());
    }
  }
}

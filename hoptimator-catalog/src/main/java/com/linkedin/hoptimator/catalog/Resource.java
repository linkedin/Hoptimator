package com.linkedin.hoptimator.catalog;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * Represents something required by a Table.
 * <p>
 * In Hoptimator, Tables can come with "baggage" in the form of Resources,
 * which are essentially YAML files. Each Resource is rendered into YAML
 * with a specific Template. Thus, it's possible to represent Kafka topics,
 * Brooklin datastreams, Flink jobs, and so on, as part of a Table, just
 * by including a corresponding Resource.
 * <p>
 * Resources are injected into a Pipeline by the planner as needed. Generally,
 * each Resource Template corresponds to a Kubernetes controller.
 * <p>
 * Resources may optionally link to input Resources, which is used strictly
 * for informational/debugging purposes.
 */
public abstract class Resource {
  private final String template;
  private final SortedMap<String, Supplier<String>> properties = new TreeMap<>();
  private final List<Resource> inputs = new ArrayList<>();

  /** A Resource which should be rendered with the given template */
  public Resource(String template) {
    this.template = template;
    export("id", this::id);
  }

  /** Copy constructor */
  public Resource(Resource other) {
    this.template = other.template;
    this.properties.putAll(other.properties);
    this.inputs.addAll(other.inputs);
  }

  /** The name of the template to render this Resource with */
  public String template() {
    return template;
  }

  /** A reasonably unique ID, based on a hash of the exported properties. */
  public String id() {
    return Integer.toHexString(hashCode());
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
    export(key,
        values.entrySet().stream().map(x -> x.getKey() + ": " + x.getValue()).collect(Collectors.joining("\n")));
  }

  /** Export a list of values */
  protected void export(String key, List<String> values) {
    export(key, String.join("\n", values));
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
  public Collection<String> render(TemplateFactory templateFactory) {
    try {
      List<String> res = new ArrayList<>();
      for (Template template : templateFactory.find(this)) {
        res.add(template.render(this));
      }
      return res;
    } catch (Exception e) {
      throw new RuntimeException("Error rendering " + template, e);
    }
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

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Resource other = (Resource) obj;
    return Objects.equals(template, other.template) && Objects.equals(properties, other.properties) && Objects.equals(
        inputs, other.inputs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ template: ").append(template()).append(" ");
    for (Map.Entry<String, Supplier<String>> entry : properties.entrySet()) {
      if (entry.getKey().equals("id")) {
        // special case for "id" to avoid recursion
        continue;
      }
      String value = entry.getValue().get();
      if (value == null || value.isEmpty()) {
        continue;
      }
      sb.append(entry.getKey());
      sb.append(":");
      sb.append(entry.getValue().get());
      sb.append(" ");
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

    String getOrDefault(String key, Supplier<String> f);

    default Environment orElse(Environment other) {
      return (k, f) -> getOrDefault(k, () -> other.getOrDefault(k, f));
    }

    default Environment orIgnore() {
      return orElse(new DummyEnvironment());
    }
  }

  /** Basic Environment implementation */
  public static class SimpleEnvironment implements Environment {
    private final Map<String, String> vars = new HashMap<>();

    public SimpleEnvironment() {
    }

    public SimpleEnvironment(Properties properties) {
      properties.forEach((k, v) -> vars.put(k.toString(), v.toString()));
    }

    public SimpleEnvironment(Map<String, String> vars) {
      exportAll(vars);
    }

    protected void export(String property, String value) {
      vars.put(property, value);
    }

    protected void exportAll(Map<String, String> properties) {
      vars.putAll(properties);
    }

    public SimpleEnvironment with(String key, String value) {
      Map<String, String> newVars = new HashMap<>(vars);
      newVars.put(key, value);
      return new SimpleEnvironment() {{
        exportAll(newVars);
      }};
    }

    @Override
    public String getOrDefault(String key, Supplier<String> f) {
      if (!vars.containsKey(key)) {
        if (f == null || f.get() == null) {
          throw new IllegalArgumentException("No variable '" + key + "' found in the environment");
        } else {
          return f.get();
        }
      }
      return vars.get(key);
    }
  }

  /** Returns "{{key}}" for any key without a default */
  public static class DummyEnvironment implements Environment {
    @Override
    public String getOrDefault(String key, Supplier<String> f) {
      if (f != null && f.get() != null) {
        return f.get();
      } else {
        return "{{" + key + "}}";
      }
    }
  }

  /** Provides access to the process's environment variables */
  public static class ProcessEnvironment implements Environment {

    @Override
    public String getOrDefault(String key, Supplier<String> f) {
      String value = System.getenv(key);
      if (value == null) {
        value = System.getProperty(key);
      }
      if (value == null && f != null) {
        value = f.get();
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
   * <p>
   * Resource-scoped variables take precedence over Environment-scoped
   * variables.
   * <p>
   * Default values can be supplied with `{{var:default}}`.
   * <p>
   * Built-in transformations can be applied to variables, including:
   * <p>
   *   - `{{var toName}}`, `{{var:default toName}}`: canonicalize the
   *     variable as a valid K8s object name.
   *   - `{{var toUpperCase}}`, `{{var:default toUpperCase}}`: render in
   *      all upper case.
   *   - `{{var toLowerCase}}`, `{{var:default toLowerCase}}`: render in
   *     all lower case.
   *   - `{{var concat}}`, `{{var:default concat}}`: concatenate a multiline
   *     string into one line
   *   - `{{var concat toUpperCase}}`: apply both transformations in sequence.
   * <p>
   * If `var` contains multiple lines, the behavior depends on context;
   * specifically, whether the pattern appears within a list or comment
   * (prefixed with `-` or `#`). For example, if the template includes:
   * <p>
   *   - {{var}}
   * <p>
   * ...and `var` contains multiple lines, then the output will be:
   * <p>
   *   - value line 1
   *   - value line 2
   * <p>
   * To avoid this behavior (and just get a multiline string), use one of
   * YAML's multiline markers, e.g.
   * <p>
   *   - |
   *       {{var}}
   * <p>
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
      Pattern p =
          Pattern.compile("([\\s\\-\\#]*)\\{\\{\\s*([\\w_\\-\\.]+)\\s*(:([\\w_\\-\\.]+))?\\s*((\\w+\\s*)*)\\s*\\}\\}");
      Matcher m = p.matcher(template);
      while (m.find()) {
        String prefix = m.group(1);
        if (prefix == null) {
          prefix = "";
        }
        String key = m.group(2);
        String defaultValue = m.group(4);
        String transform = m.group(5);
        String value = resource.getOrDefault(key, () -> env.getOrDefault(key, () -> defaultValue));
        if (value == null) {
          throw new IllegalArgumentException(template + " has no value for key " + key + ".");
        }
        String transformedValue = applyTransform(value, transform);
        String quotedPrefix = Matcher.quoteReplacement(prefix);
        String quotedValue = Matcher.quoteReplacement(transformedValue);
        String replacement = quotedPrefix + quotedValue.replaceAll("\\n", quotedPrefix);
        m.appendReplacement(sb, replacement);
      }
      m.appendTail(sb);
      return sb.toString();
    }

    private static String applyTransform(String value, String transform) {
      String res = value;
      String[] funcs = transform.split("\\W+");
      for (String f : funcs) {
        switch (f) {
          case "toLowerCase":
            res = res.toLowerCase(Locale.ROOT);
            break;
          case "toUpperCase":
            res = res.toUpperCase(Locale.ROOT);
            break;
          case "toName":
            res = canonicalizeName(res);
            break;
          case "concat":
            res = res.replace("\n", "");
            break;
          default:
            break;
        }
      }
      return res;
    }

    /** Attempt to format s as a K8s object name, or part of one. */
    protected static String canonicalizeName(String s) {
      return Names.canonicalize(s);
    }
  }

  /** Locates a Template for a given Resource */
  public interface TemplateFactory {
    Collection<Template> find(Resource resource) throws IOException;

    default Collection<String> render(Resource resource) throws IOException {
      return find(resource).stream().map(x -> x.render(resource)).collect(Collectors.toList());
    }
  }

  /** Finds Templates for a given Resource by looking for resource files in the classpath. */
  public static class SimpleTemplateFactory implements TemplateFactory {
    private final Environment env;

    public SimpleTemplateFactory(Environment env) {
      this.env = env;
    }

    @Override
    public Collection<Template> find(Resource resource) throws IOException {
      String template = resource.template();
      List<Template> res = new ArrayList<>();
      for (Enumeration<URL> e = getClass().getClassLoader().getResources(template + ".yaml.template");
          e.hasMoreElements(); ) {
        Reader reader = new InputStreamReader(e.nextElement().openStream(), StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter("\n");
        while (scanner.hasNext()) {
          sb.append(scanner.next());
          sb.append("\n");
        }
        res.add(new SimpleTemplate(env, sb.toString()));
      }
      return res;
    }
  }
}

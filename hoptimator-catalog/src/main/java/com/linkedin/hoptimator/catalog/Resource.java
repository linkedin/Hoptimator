package com.linkedin.hoptimator.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Scanner;
import java.util.function.Supplier;
import java.io.InputStream;

/** Represents something required by a Table.
 *
 *  In Hoptimator, Tables can come along with baggage in the form of Resources,
 *  which are essentially YAML files. Each Resource is rendered into YAML
 *  with a specific Template. Thus, it's possible to represent Kafka topics,
 *  Brooklin datastreams, Flink jobs, and so on, as part of a Table, just
 *  by including a corresponding Resource.
 *
 *  Resources are injected into a Pipeline by the planner as needed. Generally,
 *  each Resource Template corresponds to a K8s operator/controller.
 */
public abstract class Resource {
  private final String template;
  private final Map<String, Supplier<String>> properties = new HashMap<>();

  public Resource(String name, String template) {
    this(template);
    export("name", name);
  }

  public Resource(String template) {
    this.template = template;
  }

  protected void export(String key, Supplier<String> supplier) {
    properties.put(key, supplier);
  }

  protected void export(String key, String value) {
    properties.put(key, () -> value);
  }

  public String template() {
    return template;
  }

  public String property(String key) {
    return getOrDefault(key, null);
  }

  public Set<String> keys() {
    return properties.keySet();
  }

  public String name() {
    return getOrDefault("name", () -> "(no name)");
  }

  public String render(TemplateFactory templateFactory) {
    return templateFactory.get(this).render(this);
  }

  public String render(Template template) {
    return template.render(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ ");
    for (Map.Entry<String, Supplier<String>> entry : properties.entrySet()) {
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
      throw new IllegalArgumentException("Resource '" + name() + "' missing variable '" + key + "'");
    }
    return computed;
  }

  /** Exposes environment variables to templates */
  public interface Environment {
    Environment EMPTY = new SimpleEnvironment();
    Environment PROCESS = new ProcessEnvironment();

    String get(String key);
  }

  public static class TestEnvironment implements Environment {

    @Override
    public String get(String key) {
      return "test";
    }
  }

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

  /** Replaces `{{var}}` in a template file with the corresponding variable.
   *
   *  Resource-scoped variables take precedence over Environment-scoped variables.
   *
   *  If `var` contains multiple lines, the behavior depends on context; specifically,
   *  whether the pattern appears within a list or comment (prefixed with `-` or `#`).
   *  For example, if the template includes:
   *
   *    - {{var}}
   *
   *  ...and `var` contains multiple lines, then the output will be:
   *
   *    - value line 1
   *    - value line 2
   *
   *  To avoid this behavior (and just get a multiline string), use one of YAML's multiline
   *  markers, e.g.
   *
   *    - |
   *        {{var}}
   *
   *  In either case, the multiline string will be properly indented.
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
 
  /** Finds a Template for a given Resource by looking for resource files in the jar. */ 
  public static class SimpleTemplateFactory implements TemplateFactory {
    private final Environment env;

    public SimpleTemplateFactory(Environment env) {
      this.env = env;
    }

    @Override
    public Template get(Resource resource) {
      String template = resource.template();
      InputStream in = getClass().getClassLoader().getResourceAsStream(template + ".yaml.template");
      if (in == null) {
        throw new IllegalArgumentException("No template '" + template + "' found in jar resources");
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

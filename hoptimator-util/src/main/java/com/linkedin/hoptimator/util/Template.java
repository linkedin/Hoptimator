package com.linkedin.hoptimator.util;

import java.util.Locale;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A convenient way to generate K8s YAML. */
public interface Template {

  String render(Environment env);

  /** Exposes environment variables to templates */
  interface Environment {
    SimpleEnvironment EMPTY = new SimpleEnvironment();
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
  class SimpleEnvironment implements Environment {
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
      Map<String, String> thisVars = this.vars;
      return new SimpleEnvironment(){{
        exportAll(thisVars);
        export(key, value);
      }};
    }

    public SimpleEnvironment with(Map<String, String> values) {
      Map<String, String> thisVars = this.vars;
      return new SimpleEnvironment() {{
        exportAll(thisVars);
        exportAll(values);
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
  class DummyEnvironment implements Environment {
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
  class ProcessEnvironment implements Environment {

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

  /**
   * Replaces `{{var}}` in a template file with the corresponding variable.
   *
   * Default values can supplied with `{{var:default}}`.
   *
   * Built-in transformations can be applied to variables, including:
   *
   *   - `{{var toName}}`, `{{var:default toName}}`: canonicalize the
   *     variable as a valid K8s object name.
   *   - `{{var toUpperCase}}`, `{{var:default toUpperCase}}`: render in
   *      all upper case.
   *   - `{{var toLowerCase}}`, `{{var:default toLowerCase}}`: render in
   *     all lower case.
   *   - `{{var concat}}`, `{{var:default concat}}`: concatinate a multiline
   *     string into one line
   *   - `{{var concat toUpperCase}}`: apply both transformations in sequence.
   *
   * If `var` contains multiple lines, the behavior depends on context;
   * specifically, whether the pattern appears within a list or comment
   * (prefixed with `-` or `#`). For example, if the template includes:
   *
   *   - {{var}}
   *
   * ...and `var` contains multiple lines, then the output will be:
   *
   *   - value line 1
   *   - value line 2
   *
   * To avoid this behavior (and just get a multiline string), use one of
   * YAML's multiline markers, e.g.
   *
   *   - |
   *       {{var}}
   *
   * In either case, the multiline string will be properly indented.
   */
  class SimpleTemplate implements Template {
    private final String template;

    public SimpleTemplate(String template) {
      this.template = template;
    }
        
    @Override
    public String render(Environment env) {
      StringBuffer sb = new StringBuffer();
      Pattern p = Pattern.compile(
        "([\\s\\-\\#]*)\\{\\{\\s*([\\w_\\-\\.]+)\\s*(:([\\w_\\-\\.]+))?\\s*((\\w+\\s*)*)\\s*\\}\\}");
      Matcher m = p.matcher(template);
      while (m.find()) {
        String prefix = m.group(1);
        if (prefix == null) {
          prefix = "";
        }
        String key = m.group(2);
        String defaultValue = m.group(4);
        String transform = m.group(5);
        String value = env.getOrDefault(key, () -> defaultValue);
        if (value == null) {
          throw new IllegalArgumentException(template + " has no value for variable " + key + ".");
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
        case "concat":
          res = res.replace("\n", "");
          break;
        default:
        }
      }
      return res;
    }
  }
}

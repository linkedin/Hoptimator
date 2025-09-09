package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.ThrowingSupplier;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/** A convenient way to generate K8s YAML. */
public interface Template {
  Logger log = LoggerFactory.getLogger(Template.class);
  String render(Environment env) throws SQLException;

  /** Exposes environment variables to templates */
  interface Environment {
    SimpleEnvironment EMPTY = new SimpleEnvironment();
    Environment PROCESS = new ProcessEnvironment();

    String getOrDefault(String key, ThrowingSupplier<String> f) throws SQLException;

    default Environment orElse(Environment other) throws SQLException {
      return (k, f) -> getOrDefault(k, () -> other.getOrDefault(k, f));
    }

    default Environment orIgnore() throws SQLException {
      return orElse(new DummyEnvironment());
    }
  }

  /** Basic Environment implementation */
  class SimpleEnvironment implements Environment {
    private final Map<String, ThrowingSupplier<String>> vars;

    public SimpleEnvironment() {
      this.vars = new LinkedHashMap<>();
    }

    public SimpleEnvironment(Map<String, ThrowingSupplier<String>> vars) {
      this.vars = vars;
    }

    protected void export(String key, String value) {
      vars.put(key, () -> value);
    }

    protected void export(String key, ThrowingSupplier<String> supplier) {
      vars.put(key, supplier);
    }

    protected void exportAll(Map<String, String> properties) {
      properties.forEach((k, v) -> vars.put(k, () -> v));
    }

    public SimpleEnvironment with(String key, String value) {
      return new SimpleEnvironment(vars) {{
        export(key, value);
      }};
    }

    public SimpleEnvironment with(Map<String, String> values) {
      return new SimpleEnvironment(vars) {{
        exportAll(values);
      }};
    }

    public SimpleEnvironment with(String key, Map<String, String> values) {
      return new SimpleEnvironment(vars) {{
        export(key, formatMapAsString(values));
      }};
    }

    public SimpleEnvironment with(String key, Properties values) {
      return new SimpleEnvironment(vars) {{
        export(key, formatPropertiesAsString(values));
      }};
    }

    public SimpleEnvironment with(String key, ThrowingSupplier<String> supplier) {
      return new SimpleEnvironment(vars) {{
        export(key, supplier);
      }};
    }

    @Override
    public String getOrDefault(String key, ThrowingSupplier<String> f) throws SQLException {
      try {
        String result = vars.containsKey(key) ? vars.get(key).get() : null;
        if (result == null) {
          throw new IllegalArgumentException("No variable '" + key + "' found in the environment");
        }
        return result;
      } catch (Exception e) {
        String result = f != null ? f.get() : null;
        if (result == null) {
          throw e;
        } else {
          return result;
        }
      }
    }

    private String formatMapAsString(Map<String, String> configMap) {
      StringBuilder stringBuilder = new StringBuilder();
      for (Map.Entry<String, String> entry : configMap.entrySet()) {
        stringBuilder.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      }
      return stringBuilder.toString();
    }

    private String formatPropertiesAsString(Properties props) {
      StringBuilder stringBuilder = new StringBuilder();
      for (String key : props.stringPropertyNames()) {
        stringBuilder.append(key).append(": '").append(props.getProperty(key)).append("'\n");
      }
      return stringBuilder.toString();
    }
  }

  /** Returns "{{key}}" for any key without a default */
  class DummyEnvironment implements Environment {
    @Override
    public String getOrDefault(String key, ThrowingSupplier<String> f) throws SQLException {
      String result = f != null ? f.get() : null;
      if (result != null) {
        return result;
      } else {
        return "{{" + key + "}}";
      }
    }
  }

  /** Provides access to the process's environment variables */
  class ProcessEnvironment implements Environment {

    @Override
    public String getOrDefault(String key, ThrowingSupplier<String> f) throws SQLException {
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
  class SimpleTemplate implements Template {
    private final String template;

    public SimpleTemplate(String template) {
      this.template = template;
    }

    @Override
    public String render(Environment env) throws SQLException {
      StringBuilder sb = new StringBuilder();
      Pattern p =
          Pattern.compile("([\\s\\-\\#]*)\\{\\{\\s*([\\w_\\-\\.]+)\\s*(:([\\w_\\-\\.]*))?\\s*((\\w+\\s*)*)\\s*\\}\\}");
      Matcher m = p.matcher(template);
      while (m.find()) {
        String prefix = m.group(1);
        if (prefix == null) {
          prefix = "";
        }
        String key = m.group(2);
        String defaultValue = m.group(4);
        String transform = m.group(5);
        String value;
        try {
          value = env.getOrDefault(key, () -> defaultValue);
          if (value == null) {
            log.warn("Template variable '{}' resolved to null. Skipping template.", key);
            return null;
          } else if (transform.contains("notPresent")) {
            return null;
          }
        } catch (IllegalArgumentException e) {
          if (transform.contains("notPresent")) {
            m.appendReplacement(sb, "");
            continue;
          }
          log.warn("Missing template variable '{}' in environment: {}. Skipping template.", key, e.getMessage());
          return null;
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

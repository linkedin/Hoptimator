package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


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

    protected void exportAll(Properties properties) {
      for (String key : properties.stringPropertyNames()) {
        vars.put(key, () -> properties.getProperty(key));
      }
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

    public SimpleEnvironment with(Properties values) {
      return new SimpleEnvironment(vars) {{
        exportAll(values);
      }};
    }

    public SimpleEnvironment with(String key, Map<String, String> values) {
      return new SimpleEnvironment(vars) {{
        export(key, () -> formatMapAsString(values));
      }};
    }

    public SimpleEnvironment with(String key, Properties values) {
      return new SimpleEnvironment(vars) {{
        export(key, () -> formatPropertiesAsString(values));
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
      return new Yaml().dump(configMap);
    }

    private String formatPropertiesAsString(Properties props) {
      return props.stringPropertyNames().stream()
          .map(key -> key + ": '" + props.getProperty(key) + "'")
          .collect(Collectors.joining("\n"));
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
   * Conditional rendering of a template can be done with `{{var==value}}` or `{{var!=value}}`.
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
    private static final Pattern PATTERN =
        Pattern.compile("([\\s\\-\\#]*)\\{\\{\\s*([\\w_\\-\\.]+)\\s*((:|==|!=)([\\w_\\-\\.]*))?\\s*((\\w+\\s*)*)\\s*\\}\\}");

    private final List<Token> tokens;

    public SimpleTemplate(String template) {
      this.tokens = parse(template);
    }

    @Override
    public String render(Environment env) throws SQLException {
      // Conditional guards ({{var==value}} / {{var!=value}}) decide whether the template is used at
      // all, so they must be settled before any other variable is expanded — expanding a variable
      // can be expensive or can throw for reasons irrelevant to a skipped template (e.g. a Flink SQL
      // body that fails type validation must not break a sibling Beam template guarded off anyway).
      // Guards can appear anywhere in the text, so evaluate every guard before rendering anything.
      for (Token token : tokens) {
        if (token.isGuard() && !guardHolds(token, env)) {
          return null;
        }
      }
      StringBuilder sb = new StringBuilder();
      for (Token token : tokens) {
        if (token.literal != null) {
          sb.append(token.literal);
        } else if (token.isGuard()) {
          // Guard already validated above; the marker itself renders as nothing.
          continue;
        } else {
          String value = resolve(token, env);
          if (value == null) {
            return null;
          }
          String transformed = applyTransform(value, token.transform);
          sb.append(token.prefix).append(transformed.replace("\n", token.prefix));
        }
      }
      return sb.toString();
    }

    /** Splits the template once into an ordered list of literal spans and {@code {{...}}} tokens. */
    private static List<Token> parse(String template) {
      List<Token> tokens = new ArrayList<>();
      Matcher m = PATTERN.matcher(template);
      int last = 0;
      while (m.find()) {
        if (m.start() > last) {
          tokens.add(Token.literal(template.substring(last, m.start())));
        }
        String prefix = m.group(1) == null ? "" : m.group(1);
        tokens.add(Token.placeholder(prefix, m.group(2), m.group(4), m.group(5), m.group(6)));
        last = m.end();
      }
      if (last < template.length()) {
        tokens.add(Token.literal(template.substring(last)));
      }
      return tokens;
    }

    /** Whether a guard token's condition holds; false (skip the template) if unmet or var missing. */
    private static boolean guardHolds(Token token, Environment env) throws SQLException {
      String value;
      try {
        value = env.getOrDefault(token.key, () -> null);
      } catch (IllegalArgumentException e) {
        log.warn("Missing template variable '{}' in environment: {}. Skipping template.", token.key, e.getMessage());
        return false;
      }
      return "==".equals(token.condition) == value.equals(token.conditionValue);
    }

    /** Resolves a value token ({@code {{var}}} or {@code {{var:default}}}); null means skip. */
    private static String resolve(Token token, Environment env) throws SQLException {
      try {
        if (token.condition == null) {
          return env.getOrDefault(token.key, () -> null);
        }
        if (":".equals(token.condition)) {
          return env.getOrDefault(token.key, () -> token.conditionValue);
        }
        throw new IllegalArgumentException("Invalid template condition: " + token.condition);
      } catch (IllegalArgumentException e) {
        log.warn("Missing template variable '{}' in environment: {}. Skipping template.", token.key, e.getMessage());
        return null;
      }
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

    /**
     * One parsed piece of a template: either a literal span ({@code literal != null}) or a
     * {@code {{...}}} placeholder. A placeholder is a guard when its condition is {@code ==}/{@code !=};
     * otherwise it is a plain value ({@code condition == null}) or a defaulted value ({@code :}).
     */
    private static final class Token {
      private final String literal;
      private final String prefix;
      private final String key;
      private final String condition;
      private final String conditionValue;
      private final String transform;

      private Token(String literal, String prefix, String key, String condition,
          String conditionValue, String transform) {
        this.literal = literal;
        this.prefix = prefix;
        this.key = key;
        this.condition = condition;
        this.conditionValue = conditionValue;
        this.transform = transform;
      }

      static Token literal(String text) {
        return new Token(text, null, null, null, null, null);
      }

      static Token placeholder(String prefix, String key, String condition, String conditionValue,
          String transform) {
        return new Token(null, prefix, key, condition, conditionValue, transform);
      }

      boolean isGuard() {
        return "==".equals(condition) || "!=".equals(condition);
      }
    }
  }
}

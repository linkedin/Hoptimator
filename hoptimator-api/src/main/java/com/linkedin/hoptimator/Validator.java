package com.linkedin.hoptimator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;


public interface Validator extends Validated {

  static void validateSubdomainName(String s, Issues issues) {
    // N.B. we don't aim to be efficient here; rather, as verbose as possible.
    if (s.matches(".*\\p{Upper}.*")) {
      issues.error("Value includes uppercase letter");
    }
    if (s.matches(".*_.*")) {
      issues.error("Value includes underscore");
    }
    if (s.matches(".*\\..*")) {
      issues.error("Value includes period");
    }
  }

  static <T, U> void validateUnique(Collection<T> collection, Function<T, U> accessor, Issues issues) {
    Set<U> keys = new HashSet<U>();
    for (T t : collection) {
      U u = accessor.apply(t);
      if (keys.contains(u)) {
        issues.error("Duplicate value `" + u.toString() + "`");
      } else {
        keys.add(u);
      }
    }
  }

  static void notYetImplemented(Issues issues) {
    issues.warn("Validation not implemented for this object");
  }

  /** Validator that invokes `validate()` on the target object. */
  class DefaultValidator<T extends Validated> implements Validator {

    private final T t;

    public DefaultValidator(T t) {
      this.t = t;
    }

    @Override
    public void validate(Issues issues) {
      t.validate(issues.child(t.getClass().getSimpleName()));
    }
  }

  /** A tree of issues, organized into contexts */
  class Issues implements AutoCloseable {
    private final String context;
    private final List<String> issues = new ArrayList<>();
    private final Issues parent;
    private final Map<String, Issues> children = new HashMap<>();
    private boolean closed = false;
    private boolean valid = true;

    private Issues(String context, Issues parent) {
      this.context = context;
      this.parent = parent;
    }

    public Issues(String context) {
      this(context, null);
    }

    public Issues child(String context) {
      if (children.containsKey(context)) {
        return children.get(context);
      }
      Issues child = new Issues(context, this);
      children.put(context, child);
      return child;
    }

    public void info(String message) {
      emit(message);
    }

    public void warn(String message) {
      emit("WARNING: " + message);
    }

    public void error(String message) {
      emit("ERROR: " + message);
      invalidate();
    }

    /** Whether there are any errors */
    public boolean valid() {
      return valid;
    }

    /** For convenience only, enabling try-with-resources */
    public void close() {
      closed = true;
      children.values().forEach(x -> x.checkClosed());
    }

    private void emit(String message) {
      if (closed) {
        throw new IllegalStateException("Issues context " + fullPath() + " has been closed.");
      } else {
        issues.add(message);
      }
    }

    private void invalidate() {
      valid = false;
      if (parent != null) {
        parent.invalidate();
      }
    }

    private void checkClosed() {
      if (!closed) {
        throw new IllegalStateException("Issues context " + fullPath() + " was not closed. This indicates a bug.");
      }
    }

    private boolean empty() {
      return issues.isEmpty() && children.values().stream().allMatch(x -> x.empty());
    }

    private String fullPath() {
      List<String> parts = new ArrayList<>();
      Issues pos = this;
      while (pos != null) {
        parts.add(pos.context);
        pos = pos.parent;
      }
      Collections.reverse(parts);
      return String.join("/", parts);
    }

    private String format(int indentLevel) {
      if (empty()) {
        return "";
      }
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < indentLevel; i++) {
        sb.append("  ");
      }
      if (context != null && !context.isEmpty()) {
        sb.append(context);
        sb.append(":\n");
      }
      for (String s : issues) {
        for (int i = 0; i < indentLevel; i++) {
          sb.append("  ");
        }
        sb.append("- ");
        sb.append(s);
        sb.append("\n");
      }
      for (Issues child : children.values()) {
        sb.append(child.format(indentLevel + 1));
      }
      return sb.toString();
    }

    @Override
    public String toString() {
      return format(0);
    }
  }
}

package com.linkedin.hoptimator.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;


/**
 * Preprocesses SQL to convert $$-delimited strings to standard single-quoted strings.
 *
 * <p>This enables PostgreSQL-style dollar-quoting for inline code blocks, e.g.:
 * <pre>{@code
 * CREATE FUNCTION foo LANGUAGE PYTHON AS 'foo' IN $$
 * def foo(s):
 *     return s[::-1]
 * $$;
 * }</pre>
 *
 * <p>The preprocessor converts {@code $$...$$} to {@code '...'}, escaping any
 * internal single quotes by doubling them ({@code '} → {@code ''}).
 */
final class DollarQuoting {

  private DollarQuoting() {
  }

  /** Preprocesses a Reader, converting $$-delimited strings to single-quoted SQL strings. */
  static Reader preprocess(Reader reader) {
    try {
      StringBuilder sb = new StringBuilder();
      char[] buf = new char[4096];
      int n;
      while ((n = reader.read(buf)) != -1) {
        sb.append(buf, 0, n);
      }
      return new StringReader(preprocess(sb.toString()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Converts $$-delimited strings to single-quoted SQL strings. */
  static String preprocess(String sql) {
    int idx = sql.indexOf("$$");
    if (idx < 0) {
      return sql;
    }
    StringBuilder result = new StringBuilder();
    int i = 0;
    while (i < sql.length()) {
      if (i + 1 < sql.length() && sql.charAt(i) == '$' && sql.charAt(i + 1) == '$') {
        int end = sql.indexOf("$$", i + 2);
        if (end < 0) {
          throw new IllegalArgumentException("Unclosed $$ delimiter in SQL");
        }
        String content = sql.substring(i + 2, end);
        result.append('\'');
        result.append(content.replace("'", "''"));
        result.append('\'');
        i = end + 2;
      } else {
        result.append(sql.charAt(i));
        i++;
      }
    }
    return result.toString();
  }
}

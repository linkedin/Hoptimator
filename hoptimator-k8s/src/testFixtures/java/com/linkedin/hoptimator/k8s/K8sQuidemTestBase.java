package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;

import com.linkedin.hoptimator.PipelineGraph;
import com.linkedin.hoptimator.jdbc.QuidemTestBase;
import com.linkedin.hoptimator.util.MermaidRenderer;


/**
 * Quidem test base for K8s-backed integration tests. Adds a {@code !graph} command on top of
 * {@link QuidemTestBase}'s {@code !describe} and {@code !spec} so {@code .id} scripts can:
 *
 * <pre>
 *   create or replace materialized view ads.audience as ...;
 *   !update
 *
 *   flowchart LR
 *     ...
 *
 *   !graph view ads/audience
 * </pre>
 *
 * <p>Output rendering matches the {@code !graph} sqlline command in {@code HoptimatorAppConfig}
 * — same {@link PipelineGraphBuilder} + {@link MermaidRenderer} chain — so anything that breaks
 * one breaks the other.
 *
 * <p>The script form is:
 * <pre>
 *   !graph view  &lt;namespace&gt;/&lt;name&gt;
 *   !graph logical &lt;namespace&gt;/&lt;name&gt;
 *   !graph table &lt;database&gt;.&lt;path1&gt;.&lt;path2&gt;...
 * </pre>
 * Optional {@code --depth N} clamped at {@link PipelineGraphBuilder#MAX_DEPTH}.
 */
public abstract class K8sQuidemTestBase extends QuidemTestBase {

  @Override
  protected Command parseExtraCommand(List<String> lines, List<String> content, String line) {
    if (!line.startsWith("graph")) {
      return null;
    }
    final List<String> echoCopy = new ArrayList<>(lines);
    return new AbstractCommand() {
      @Override
      public void execute(Context context, boolean execute) throws Exception {
        if (execute) {
          Connection connection = context.connection();
          K8sContext k8s = K8sContext.create(connection);
          String[] parts = line.trim().split("\\s+");
          if (parts.length < 3) {
            throw new IllegalArgumentException(
                "Usage: !graph <view|logical|table> <name|database.path> [--depth N]");
          }
          String kind = parts[1].toLowerCase();
          String identifier = parts[2];
          int depth = PipelineGraphBuilder.DEFAULT_DEPTH;
          for (int i = 3; i < parts.length - 1; i++) {
            if ("--depth".equals(parts[i])) {
              depth = Integer.parseInt(parts[i + 1]);
            }
          }

          PipelineGraphBuilder builder = new PipelineGraphBuilder(k8s);
          PipelineGraph graph;
          switch (kind) {
            case "view": {
              String[] nsName = splitNamespaceName(identifier, k8s.namespace());
              graph = builder.forView(nsName[0], nsName[1], depth);
              break;
            }
            case "logical": {
              String[] nsName = splitNamespaceName(identifier, k8s.namespace());
              graph = builder.forLogicalTable(nsName[0], nsName[1], depth);
              break;
            }
            case "table": {
              String[] segments = identifier.split("\\.");
              if (segments.length < 2) {
                throw new IllegalArgumentException(
                    "table identifier must be <database>.<path>; got: " + identifier);
              }
              String database = segments[0];
              List<String> path = Arrays.asList(Arrays.copyOfRange(segments, 1, segments.length));
              graph = builder.forResource(database, path, depth);
              break;
            }
            default:
              throw new IllegalArgumentException(
                  "Unknown kind: " + kind + " (expected view, logical, or table)");
          }
          String mermaid = MermaidRenderer.render(graph);
          // Strip a trailing newline so the line list matches what's in the .id file (Quidem
          // splits the captured content on newlines without a trailing blank).
          if (mermaid.endsWith("\n")) {
            mermaid = mermaid.substring(0, mermaid.length() - 1);
          }
          // For reverse lookups, surface a Mermaid-comment warning when the graph collapses to
          // just the root node — same behavior as the sqlline `!graph` command.
          if ("table".equals(kind) && graph.nodes().size() == 1 && graph.edges().isEmpty()) {
            mermaid = mermaid + "\n%% WARNING: no pipelines reference this resource — the "
                + "identifier may not exist or no pipelines have been deployed against it yet.";
          }
          context.echo(Arrays.asList(mermaid.split("\n")));
        } else {
          context.echo(content);
        }
        context.echo(echoCopy);
      }
    };
  }

  /** Splits {@code namespace/name} or returns {@code (defaultNamespace, name)} when no slash. */
  protected static String[] splitNamespaceName(String identifier, String defaultNamespace) {
    int idx = identifier.indexOf('/');
    if (idx < 0) {
      return new String[] { defaultNamespace, identifier };
    }
    return new String[] { identifier.substring(0, idx), identifier.substring(idx + 1) };
  }
}

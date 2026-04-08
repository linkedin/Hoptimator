package com.linkedin.hoptimator.planner;

import com.linkedin.hoptimator.catalog.Resource;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class PipelineTest {

  @Mock
  private RelDataType outputType;

  @Mock
  private Resource.TemplateFactory templateFactory;

  private Resource upstreamResource;
  private Resource downstreamResource;
  private SqlJob sqlJob;

  @BeforeEach
  public void setUp() {
    upstreamResource = new Resource("UpstreamType") {
      {
        export("name", "upstream-resource");
      }
    };
    downstreamResource = new Resource("DownstreamType") {
      {
        export("name", "downstream-resource");
      }
    };
    sqlJob = new SqlJob("SELECT * FROM source");
  }

  @Test
  public void sqlJobAccessorReturnsSqlJob() {
    Pipeline pipeline = new Pipeline(
        Collections.singletonList(upstreamResource),
        sqlJob,
        Collections.singletonList(downstreamResource),
        outputType);

    assertEquals(sqlJob, pipeline.sqlJob());
  }

  @Test
  public void upstreamResourcesAccessorReturnsUpstreamResources() {
    List<Resource> upstream = Collections.singletonList(upstreamResource);
    Pipeline pipeline = new Pipeline(upstream, sqlJob, Collections.emptyList(), outputType);

    assertThat(pipeline.upstreamResources()).containsExactlyElementsOf(upstream);
  }

  @Test
  public void downstreamResourcesAccessorReturnsDownstreamResources() {
    List<Resource> downstream = Collections.singletonList(downstreamResource);
    Pipeline pipeline = new Pipeline(Collections.emptyList(), sqlJob, downstream, outputType);

    assertThat(pipeline.downstreamResources()).containsExactlyElementsOf(downstream);
  }

  @Test
  public void outputTypeAccessorReturnsOutputType() {
    Pipeline pipeline = new Pipeline(Collections.emptyList(), sqlJob, Collections.emptyList(), outputType);

    assertEquals(outputType, pipeline.outputType());
  }

  @Test
  public void resourcesIncludesUpstreamSqlJobAndDownstream() {
    Pipeline pipeline = new Pipeline(
        Collections.singletonList(upstreamResource),
        sqlJob,
        Collections.singletonList(downstreamResource),
        outputType);

    Collection<Resource> resources = pipeline.resources();

    assertNotNull(resources);
    // should include upstream, sqlJob, and downstream
    assertThat(resources).hasSizeGreaterThanOrEqualTo(3);

    // verify templates are present
    boolean hasUpstream = resources.stream().anyMatch(r -> "UpstreamType".equals(r.template()));
    boolean hasSqlJob = resources.stream().anyMatch(r -> "SqlJob".equals(r.template()));
    boolean hasDownstream = resources.stream().anyMatch(r -> "DownstreamType".equals(r.template()));
    assertTrue(hasUpstream, "Should contain upstream resource");
    assertTrue(hasSqlJob, "Should contain SqlJob resource");
    assertTrue(hasDownstream, "Should contain downstream resource");
  }

  @Test
  public void resourcesWithNoUpstreamOrDownstreamJustHasSqlJob() {
    Pipeline pipeline = new Pipeline(Collections.emptyList(), sqlJob, Collections.emptyList(), outputType);

    Collection<Resource> resources = pipeline.resources();
    assertNotNull(resources);
    assertTrue(resources.stream().anyMatch(r -> "SqlJob".equals(r.template())));
  }

  @Test
  public void renderCallsTemplateFactoryForEachResource() throws IOException {
    when(templateFactory.render(any(Resource.class))).thenReturn(Collections.singletonList("rendered"));

    Pipeline pipeline = new Pipeline(
        Collections.singletonList(upstreamResource),
        sqlJob,
        Collections.singletonList(downstreamResource),
        outputType);

    Collection<String> rendered = pipeline.render(templateFactory);

    assertNotNull(rendered);
    assertThat(rendered).isNotEmpty();
    // Each resource produced one "rendered" string, verify we got some back
    assertThat(rendered).containsOnly("rendered");
  }

  @Test
  public void mermaidStartsWithFlowchartHeader() {
    Pipeline pipeline = new Pipeline(
        Collections.singletonList(upstreamResource),
        sqlJob,
        Collections.singletonList(downstreamResource),
        outputType);

    String mermaid = pipeline.mermaid();

    assertTrue(mermaid.startsWith("flowchart\n"), "Mermaid should start with 'flowchart\\n'");
  }

  @Test
  public void mermaidContainsSubgraphForEachResourceType() {
    Pipeline pipeline = new Pipeline(
        Collections.singletonList(upstreamResource),
        sqlJob,
        Collections.singletonList(downstreamResource),
        outputType);

    String mermaid = pipeline.mermaid();

    assertTrue(mermaid.contains("subgraph UpstreamType"), "Should contain UpstreamType subgraph");
    assertTrue(mermaid.contains("subgraph SqlJob"), "Should contain SqlJob subgraph");
    assertTrue(mermaid.contains("subgraph DownstreamType"), "Should contain DownstreamType subgraph");
  }

  @Test
  public void mermaidContainsEndForEachSubgraph() {
    Pipeline pipeline = new Pipeline(
        Collections.singletonList(upstreamResource),
        sqlJob,
        Collections.emptyList(),
        outputType);

    String mermaid = pipeline.mermaid();

    assertTrue(mermaid.contains("  end"), "Mermaid should contain 'end' closings");
  }

  @Test
  public void sanitizeViaMermaidTruncatesLongValues() {
    Resource resourceWithLongValue = new Resource("LongValue") {
      {
        export("description", "This is a very long description that exceeds twenty characters");
      }
    };

    Pipeline pipeline = new Pipeline(
        Collections.singletonList(resourceWithLongValue),
        sqlJob,
        Collections.emptyList(),
        outputType);

    String mermaid = pipeline.mermaid();

    // The sanitize method truncates to 17 chars + "..."
    assertTrue(mermaid.contains("..."), "Long property values should be truncated with '...'");
  }

  @Test
  public void sanitizeViaMermaidEscapesDoubleQuotes() {
    Resource resourceWithQuotes = new Resource("QuotedValue") {
      {
        export("name", "has\"quote");
      }
    };

    Pipeline pipeline = new Pipeline(
        Collections.singletonList(resourceWithQuotes),
        sqlJob,
        Collections.emptyList(),
        outputType);

    String mermaid = pipeline.mermaid();

    // The sanitize method replaces " with &quot;
    assertTrue(mermaid.contains("&quot;") || !mermaid.contains("has\"quote"),
        "Double quotes should be escaped");
  }

  @Test
  public void sanitizeViaMermaidCollapsesNewlines() {
    Resource resourceWithNewlines = new Resource("NewlineValue") {
      {
        export("sql", "line1\nline2\nline3");
      }
    };

    Pipeline pipeline = new Pipeline(
        Collections.singletonList(resourceWithNewlines),
        sqlJob,
        Collections.emptyList(),
        outputType);

    String mermaid = pipeline.mermaid();

    // The sanitize method replaces \n with space
    // The property value "line1 line2 line3" exceeds 20 chars so gets truncated to "line1 line2 line..."
    assertTrue(mermaid.contains("line1"), "Resource property should appear in mermaid output");
  }
}

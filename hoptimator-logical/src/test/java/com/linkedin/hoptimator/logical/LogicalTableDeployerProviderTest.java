package com.linkedin.hoptimator.logical;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LogicalTableDeployerProviderTest {

  private final LogicalTableDeployerProvider provider = new LogicalTableDeployerProvider();

  @Test
  public void priorityIsZero() {
    assertEquals(2, provider.priority());
  }

  @Test
  public void deployersReturnsEmptyWhenInputIsNotSource() {
    Deployable notASource = new Deployable() { };
    Collection<Deployer> deployers = provider.deployers(notASource, null);
    assertTrue(deployers.isEmpty());
  }

  @Test
  public void deployersReturnsEmptyWhenConnectionIsNull() {
    // When connection is null, extractPropertiesFromJdbcSchema returns null
    Source source = new Source("mydb", List.of("mydb", "myschema", "mytable"), Map.of());
    Collection<Deployer> deployers = provider.deployers(source, null);
    assertTrue(deployers.isEmpty());
  }
}

package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.DeploymentContext;

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
  public void deployersReturnsEmptyWhenDatabaseUnresolvable() {
    // A context that can't resolve the database (databaseProperties returns null) yields no deployers.
    Source source = new Source("mydb", List.of("mydb", "myschema", "mytable"), Map.of());
    DeploymentContext context = org.mockito.Mockito.mock(DeploymentContext.class);
    Collection<Deployer> deployers = provider.deployers(source, context);
    assertTrue(deployers.isEmpty());
  }
}

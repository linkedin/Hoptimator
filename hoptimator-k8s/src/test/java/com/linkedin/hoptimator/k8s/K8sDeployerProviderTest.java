package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Deployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.View;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
class K8sDeployerProviderTest {

  @Mock
  private Connection connection;

  @Mock
  private K8sContext context;

  @Mock
  private MockedStatic<K8sContext> contextStatic;

  @Test
  void priorityReturnsOne() {
    K8sDeployerProvider provider = new K8sDeployerProvider();
    assertEquals(1, provider.priority());
  }

  @Test
  void deployersForMaterializedViewReturnsMaterializedViewDeployer() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sDeployerProvider provider = new K8sDeployerProvider();
    MaterializedView mv = mock(MaterializedView.class);

    Collection<Deployer> deployers = provider.deployers(mv, connection);

    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof K8sMaterializedViewDeployer);
  }

  @Test
  void deployersForViewReturnsViewDeployer() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sDeployerProvider provider = new K8sDeployerProvider();
    View view = mock(View.class);

    Collection<Deployer> deployers = provider.deployers(view, connection);

    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof K8sViewDeployer);
  }

  @Test
  void deployersForJobReturnsJobDeployer() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sDeployerProvider provider = new K8sDeployerProvider();
    Job job = mock(Job.class);

    Collection<Deployer> deployers = provider.deployers(job, connection);

    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof K8sJobDeployer);
  }

  @Test
  void deployersForSourceReturnsSourceDeployer() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sDeployerProvider provider = new K8sDeployerProvider();
    Source source = mock(Source.class);

    Collection<Deployer> deployers = provider.deployers(source, connection);

    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof K8sSourceDeployer);
  }

  @Test
  void deployersForTriggerReturnsTriggerDeployer() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sDeployerProvider provider = new K8sDeployerProvider();
    Trigger trigger = mock(Trigger.class);

    Collection<Deployer> deployers = provider.deployers(trigger, connection);

    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof K8sTriggerDeployer);
  }

  @Test
  void deployersForUnknownTypeReturnsEmpty() {
    contextStatic.when(() -> K8sContext.create(any(Connection.class))).thenReturn(context);
    K8sDeployerProvider provider = new K8sDeployerProvider();
    Deployable unknown = mock(Deployable.class);

    Collection<Deployer> deployers = provider.deployers(unknown, connection);

    assertTrue(deployers.isEmpty());
  }
}

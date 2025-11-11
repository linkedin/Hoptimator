package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.util.Yaml;


public class FakeK8sApi<T extends KubernetesObject, U extends KubernetesListObject> extends K8sApi<T, U> {

  private final List<T> objects;

  public FakeK8sApi(List<T> objects) {
    super(null, null);
    this.objects = objects;
  }

  @Override
  public Collection<T> list() throws SQLException {
    return objects;
  }

  @Override
  public T get(String name) throws SQLException {
    return objects.stream().filter(x -> x.getMetadata().getName().equals(name)).findFirst()
        .orElseThrow(() -> new SQLException("No object named " + name, null, 404));
  }

  @Override
  public T get(String namespace, String name) throws SQLException {
    return get(name);
  }

  @Override
  public T getIfExists(String namespace, String name) throws SQLException {
    return objects.stream().filter(x -> x.getMetadata().getName().equals(name)).findFirst()
        .orElse(null);
  }

  @Override
  public T get(T obj) throws SQLException {
    return obj;
  }

  @Override
  public Collection<T> select(String labelSelector) throws SQLException {
    return objects;
  }

  @Override
  public void create(T obj) throws SQLException {
    System.out.println("Created:");
    System.out.println(Yaml.dump(obj));
    System.out.println();
    objects.add(obj);
  }

  @Override
  public void delete(T obj) throws SQLException {
    System.out.println("Deleted:");
    System.out.println(Yaml.dump(obj));
    System.out.println();
    objects.remove(obj);
  }

  @Override
  public void delete(String namespace, String name) throws SQLException {
    delete(get(namespace, name));
  }

  @Override
  public void delete(String name) throws SQLException {
    delete(null, name);
  }

  @Override
  public void update(T obj) throws SQLException {
    System.out.println("Updated:");
    System.out.println(Yaml.dump(obj));
    System.out.println();
    objects.remove(obj);
    objects.add(obj);
  }

  @Override
  public void updateStatus(T obj, Object status) throws SQLException {
    update(obj);
  }
}

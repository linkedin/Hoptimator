package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Deployable;

import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;

/**
 * A set of Deployable objects that work together to deliver data.
 */
public class Pipeline implements Deployable {

  private List<Deployable> deployables = new ArrayList<>();

  public void add(Deployable deployable) {
    deployables.add(deployable);
  }

  @Override
  public void create() throws SQLException {
    for (Deployable deployable : deployables) {
      deployable.create();
    }
  }

  @Override
  public void delete() throws SQLException {
    for (Deployable deployable : deployables) {
      deployable.delete();
    }
  }

  @Override
  public void update() throws SQLException {
    for (Deployable deployable : deployables) {
      deployable.update();
    }
  } 

  @Override
  public List<String> specify() throws SQLException {
    List<String> specs = new ArrayList<>();
    for (Deployable deployable : deployables) {
      specs.addAll(deployable.specify());
    }
    return specs;
  }
}

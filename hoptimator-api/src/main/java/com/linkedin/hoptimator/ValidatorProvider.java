package com.linkedin.hoptimator;

import java.util.Collection;


public interface ValidatorProvider {

  <T> Collection<Validator> validators(T obj);
}

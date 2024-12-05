package com.linkedin.hoptimator;

import java.util.Collection;

public interface ValidatorProvider {

  <T> Collection<Validator<T>> validators(Class<T> clazz);
}

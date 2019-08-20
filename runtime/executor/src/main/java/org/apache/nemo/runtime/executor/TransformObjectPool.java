package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.ir.vertex.transform.Transform;

import java.util.HashMap;
import java.util.Map;

public final class TransformObjectPool {

  private final Map<String, Transform> transformMap = new HashMap<>();

  public TransformObjectPool() {

  }

  public synchronized Transform getTransform(final Transform transform) {
    if (transformMap.containsKey(transform.getClass().getName())) {
      return transformMap.get(transform.getClass().getName());
    } else {
      transformMap.put(transform.getClass().getName(), transform);
      return transform;
    }
  }
}

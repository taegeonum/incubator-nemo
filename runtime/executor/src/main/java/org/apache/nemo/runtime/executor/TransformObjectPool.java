package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TransformObjectPool {
  private static final Logger LOG = LoggerFactory.getLogger(TransformObjectPool.class.getName());

  private final ConcurrentMap<String, Transform> transformMap = new ConcurrentHashMap<>();

  public TransformObjectPool() {

  }

  public Transform getTransform(final String vertexId, final Transform transform) {
    LOG.info("Get vertex transform {}/{}", vertexId, transform);
    if (transformMap.containsKey(vertexId)) {
      return transformMap.get(vertexId);
    } else {
      transformMap.putIfAbsent(vertexId, transform);
      return transformMap.get(vertexId);
    }
  }
}

package org.apache.nemo.runtime.lambdaexecutor.general;

import org.apache.nemo.runtime.executor.common.OffloadingManager;
import org.apache.nemo.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.Serializer;

public final class SimpleOffloadingManager implements OffloadingManager {

  @Override
  public void createWorker(int num) {

  }

  @Override
  public void offloading(String taskId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void deoffloading(String taskId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void offloadIntermediateData(String taskId, TaskHandlingEvent data) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void offloadSourceData(String taskId, String edgeId, Object data, Serializer serializer) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void close() {

  }
}

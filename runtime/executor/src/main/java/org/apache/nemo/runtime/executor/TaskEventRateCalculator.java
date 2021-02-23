package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskExecutorMapWrapper;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class TaskEventRateCalculator {

  private final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap;

  @Inject
  private TaskEventRateCalculator(final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.taskExecutorMap = taskExecutorMapWrapper.getTaskExecutorMap();
  }

  public int calculateProcessedEvent() {
    int sum = 0;
    for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
      final AtomicInteger count = taskExecutor.getProcessedCnt();
      final int cnt = count.get();
      sum += cnt;
      count.getAndAdd(-cnt);
    }
    return sum;
  }
}
